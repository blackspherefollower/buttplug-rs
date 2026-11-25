// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use futures::{
  FutureExt,
  StreamExt,
  future::{self, BoxFuture},
  stream,
};
use std::{
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
  },
};
use tokio::sync::broadcast::{self, error::RecvError};
use uuid::Uuid;

use crate::{
  device::hardware::{Hardware, HardwareEvent, HardwareSubscribeCmd, HardwareUnsubscribeCmd},
  message::ButtplugServerDeviceMessage,
};
use buttplug_core::{
  errors::ButtplugDeviceError,
  message::{InputReadingV4, InputType, InputTypeReading, InputValue},
  util::stream::convert_broadcast_receiver_to_stream,
};
use buttplug_server_device_config::{Endpoint, ServerDeviceDefinition};

pub type MagicMotionInputEventStream =
  Pin<Box<dyn futures::Stream<Item = ButtplugServerDeviceMessage> + Send>>;

#[derive(Clone, Copy)]
pub enum MagicMotionPressureFormat {
  /// Two little-endian u16 sensors: bytes 0/1 are right, bytes 2/3 are left.
  Dual,
  /// One little-endian u16 sensor in bytes 0/1.
  Single,
}

impl MagicMotionPressureFormat {
  /// Flamingo Max exposes two pressure features. V1 kegel devices expose a single pressure
  /// feature, as do the single-sensor devices in the newer Magic Motion protocols.
  pub fn for_device_definition(definition: &ServerDeviceDefinition) -> Self {
    if definition
      .features()
      .values()
      .filter(|feature| feature.contains_input(InputType::Pressure))
      .count()
      > 1
    {
      Self::Dual
    } else {
      Self::Single
    }
  }
}

/// Shared subscribe/unsubscribe/event handling for Magic Motion inputs.
///
/// Each notification can contain one reading per feature. Slots that are `None` are decoded but
/// not emitted, which lets formats with different feature counts share the same helper.
pub struct MagicMotionInputSubscriptions<const N: usize> {
  name: &'static str,
  endpoint: Endpoint,
  feature_indices: [Option<u32>; N],
  decode: fn(&[u8]) -> Option<[i32; N]>,
  reading: fn(i32) -> InputTypeReading,
  subscribed_features: Arc<AtomicU32>,
  generation: Arc<AtomicU64>,
  event_stream: broadcast::Sender<ButtplugServerDeviceMessage>,
}

impl<const N: usize> MagicMotionInputSubscriptions<N> {
  fn new(
    name: &'static str,
    endpoint: Endpoint,
    feature_indices: [Option<u32>; N],
    decode: fn(&[u8]) -> Option<[i32; N]>,
    reading: fn(i32) -> InputTypeReading,
  ) -> Self {
    let (sender, _) = broadcast::channel(256);
    Self {
      name,
      endpoint,
      feature_indices,
      decode,
      reading,
      subscribed_features: Arc::new(AtomicU32::new(0)),
      generation: Arc::new(AtomicU64::new(0)),
      event_stream: sender,
    }
  }

  fn feature_bit(feature_index: u32) -> Result<u32, ButtplugDeviceError> {
    1u32.checked_shl(feature_index).ok_or_else(|| {
      ButtplugDeviceError::DeviceFeatureMismatch(format!(
        "Magic Motion feature index {feature_index} exceeds 31"
      ))
    })
  }

  fn validate_feature_index(&self, feature_index: u32) -> Result<u32, ButtplugDeviceError> {
    if !self.feature_indices.contains(&Some(feature_index)) {
      return Err(ButtplugDeviceError::DeviceFeatureMismatch(format!(
        "Magic Motion {name} does not have feature index {feature_index}",
        name = self.name
      )));
    }
    Self::feature_bit(feature_index)
  }

  pub fn subscribe(
    &self,
    device_index: u32,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    let bit = match self.validate_feature_index(feature_index) {
      Ok(bit) => bit,
      Err(err) => return future::ready(Err(err)).boxed(),
    };
    let previous = self.subscribed_features.fetch_or(bit, Ordering::AcqRel);
    if previous & bit != 0 {
      debug!("Magic Motion {} already subscribed, skipping", self.name);
      return future::ready(Ok(())).boxed();
    }

    async move {
      if previous == 0 {
        if let Err(err) = device
          .subscribe(&HardwareSubscribeCmd::new(feature_id, self.endpoint))
          .await
        {
          self.subscribed_features.fetch_and(!bit, Ordering::AcqRel);
          return Err(err);
        }

        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let sender = self.event_stream.clone();
        let mut hardware_stream = device.event_stream();
        let subscribed_features = self.subscribed_features.clone();
        let current_generation = self.generation.clone();
        let name = self.name;
        let endpoint = self.endpoint;
        let feature_indices = self.feature_indices;
        let decode = self.decode;
        let reading = self.reading;
        debug!("Starting Magic Motion {name} subscription");
        buttplug_core::spawn!("Magic Motion input event handler", async move {
          let mut cached_values = [None; N];
          loop {
            let info = match hardware_stream.recv().await {
              Ok(info) => info,
              Err(RecvError::Lagged(n)) => {
                warn!("Magic Motion {name} subscription lagged, dropped {n} messages");
                continue;
              }
              Err(RecvError::Closed) => break,
            };
            let subscribed = subscribed_features.load(Ordering::Relaxed);
            if current_generation.load(Ordering::Relaxed) != generation
              || sender.receiver_count() == 0
              || subscribed == 0
            {
              return;
            }
            let HardwareEvent::Notification(_, notification_endpoint, data) = info else {
              continue;
            };
            if notification_endpoint != endpoint {
              continue;
            }

            let Some(values) = decode(&data) else {
              continue;
            };
            for (slot, value) in values.into_iter().enumerate() {
              let Some(feature_index) = feature_indices[slot] else {
                continue;
              };
              let Ok(feature_bit) = Self::feature_bit(feature_index) else {
                continue;
              };
              if subscribed & feature_bit == 0 || cached_values[slot] == Some(value) {
                continue;
              }
              if sender
                .send(InputReadingV4::new(device_index, feature_index, reading(value)).into())
                .is_err()
              {
                debug!(
                  "Hardware device listener for Magic Motion {name} shut down, returning from task"
                );
                return;
              }
              cached_values[slot] = Some(value);
            }
          }
        });
      }
      Ok(())
    }
    .boxed()
  }

  pub fn unsubscribe(
    &self,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    let bit = match self.validate_feature_index(feature_index) {
      Ok(bit) => bit,
      Err(err) => return future::ready(Err(err)).boxed(),
    };
    let previous = self.subscribed_features.fetch_and(!bit, Ordering::AcqRel);
    if previous & bit == 0 {
      return future::ready(Ok(())).boxed();
    }

    async move {
      if previous == bit {
        // Invalidate the previous event task so a later subscribe does not leave two tasks
        // emitting duplicate readings.
        self.generation.fetch_add(1, Ordering::AcqRel);
        device
          .unsubscribe(&HardwareUnsubscribeCmd::new(feature_id, self.endpoint))
          .await?;
      }
      Ok(())
    }
    .boxed()
  }

  pub fn event_stream(&self) -> MagicMotionInputEventStream {
    convert_broadcast_receiver_to_stream(self.event_stream.subscribe()).boxed()
  }
}

pub fn pressure_subscriptions(
  format: MagicMotionPressureFormat,
) -> MagicMotionInputSubscriptions<2> {
  let (feature_indices, decode) = match format {
    MagicMotionPressureFormat::Dual => (
      [Some(2), Some(3)],
      decode_dual_pressure as fn(&[u8]) -> Option<[i32; 2]>,
    ),
    MagicMotionPressureFormat::Single => (
      [Some(2), None],
      decode_single_pressure as fn(&[u8]) -> Option<[i32; 2]>,
    ),
  };
  MagicMotionInputSubscriptions::new(
    "pressure",
    Endpoint::RxPressure,
    feature_indices,
    decode,
    pressure_reading,
  )
}

pub fn accelerometer_subscriptions(
  feature_indices: [Option<u32>; 3],
) -> MagicMotionInputSubscriptions<3> {
  MagicMotionInputSubscriptions::new(
    "accelerometer",
    Endpoint::RxAccel,
    feature_indices,
    decode_accelerometer,
    accelerometer_reading,
  )
}

/// Decodes the two little-endian u16 pressure readings used by the Flamingo Max. The app calls
/// bytes 0/1 right and bytes 2/3 left, but feature order is left then right.
fn decode_dual_pressure(data: &[u8]) -> Option<[i32; 2]> {
  if data.len() < 4 {
    error!(
      "Magic Motion pressure data not expected length: {}",
      data.len()
    );
    return None;
  }

  let raw_right = u16::from_le_bytes([data[0], data[1]]) as i32;
  let raw_left = u16::from_le_bytes([data[2], data[3]]) as i32;
  Some([raw_left, raw_right])
}

/// Decodes the single little-endian u16 pressure reading used by V1 kegel devices and the newer
/// single-sensor devices. The app divides this by 10 for type-2 devices before its own UI scaling;
/// we expose the raw value instead.
fn decode_single_pressure(data: &[u8]) -> Option<[i32; 2]> {
  if data.len() < 2 {
    error!(
      "Magic Motion pressure data not expected length: {}",
      data.len()
    );
    return None;
  }

  Some([u16::from_le_bytes([data[0], data[1]]) as i32, 0])
}

/// Decodes the first three signed little-endian axes from either the 6-byte SMV45 frame or the
/// generic 18-byte frame. Later values in the 18-byte frame are unused.
pub fn decode_accelerometer(data: &[u8]) -> Option<[i32; 3]> {
  if data.len() != 6 && data.len() < 18 {
    error!(
      "Magic Motion accelerometer data not expected length: {}",
      data.len()
    );
    return None;
  }

  Some([
    i16::from_le_bytes([data[0], data[1]]) as i32,
    i16::from_le_bytes([data[2], data[3]]) as i32,
    i16::from_le_bytes([data[4], data[5]]) as i32,
  ])
}

/// Decodes the raw 32-bit step delta. The device clears this value when it is read, so each read
/// reports the steps accumulated since the previous read. Callers may accumulate these deltas
/// before exposing a cumulative counter. This matches the APK's integer interpretation of the
/// four bytes, including values above `i32::MAX`.
pub fn decode_counter(data: &[u8]) -> Option<[i32; 1]> {
  if data.len() < 4 {
    error!(
      "Magic Motion counter data not expected length: {}",
      data.len()
    );
    return None;
  }

  Some([i32::from_le_bytes([data[0], data[1], data[2], data[3]])])
}

fn pressure_reading(value: i32) -> InputTypeReading {
  InputTypeReading::Pressure(InputValue::new(value))
}

fn accelerometer_reading(value: i32) -> InputTypeReading {
  InputTypeReading::Accelerometer(InputValue::new(value))
}

pub fn select_event_streams(
  streams: Vec<MagicMotionInputEventStream>,
) -> MagicMotionInputEventStream {
  stream::select_all(streams).boxed()
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn test_decode_dual_pressure() {
    let data = [0x34, 0x12, 0x78, 0x56];
    assert_eq!(decode_dual_pressure(&data), Some([22136, 4660]));
  }

  #[test]
  fn test_decode_single_pressure() {
    let data = [0x34, 0x12];
    assert_eq!(decode_single_pressure(&data), Some([4660, 0]));
  }

  #[test]
  fn test_decode_pressure_bad_length() {
    assert_eq!(decode_dual_pressure(&[0u8; 3]), None);
    assert_eq!(decode_single_pressure(&[0u8]), None);
  }

  #[test]
  fn test_decode_accelerometer_smv45() {
    let data = [0x00, 0x01, 0xFE, 0xFF, 0xFF, 0x7F];
    assert_eq!(decode_accelerometer(&data), Some([256, -2, 32767]));
  }

  #[test]
  fn test_decode_accelerometer_generic() {
    let mut data = vec![0u8; 18];
    data[0..6].copy_from_slice(&[0xFF, 0xFF, 0x01, 0x00, 0x00, 0x80]);
    assert_eq!(decode_accelerometer(&data), Some([-1, 1, -32768]));
  }

  #[test]
  fn test_decode_accelerometer_bad_length() {
    assert_eq!(decode_accelerometer(&[0u8; 5]), None);
  }

  #[test]
  fn test_accelerometer_subscriptions_only_accept_configured_features() {
    let none = accelerometer_subscriptions([None, None, None]);
    assert!(none.validate_feature_index(0).is_err());

    let configured = accelerometer_subscriptions([Some(3), Some(4), Some(5)]);
    assert!(configured.validate_feature_index(3).is_ok());
    assert!(configured.validate_feature_index(4).is_ok());
    assert!(configured.validate_feature_index(5).is_ok());
    assert!(configured.validate_feature_index(0).is_err());
  }

  #[test]
  fn test_decode_counter() {
    assert_eq!(decode_counter(&[0x40, 0x42, 0x0F, 0x00]), Some([1_000_000]));
    assert_eq!(decode_counter(&[0xFF, 0xFF, 0xFF, 0xFF]), Some([-1]));
  }

  #[test]
  fn test_decode_counter_bad_length() {
    assert_eq!(decode_counter(&[0u8; 3]), None);
  }
}

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
};
use std::{
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
  },
};
use tokio::sync::broadcast::{self, error::RecvError};
use uuid::Uuid;

use crate::{
  device::hardware::{
    Hardware,
    HardwareEvent,
    HardwareSubscribeCmd,
    HardwareUnsubscribeCmd,
  },
  message::ButtplugServerDeviceMessage,
};
use buttplug_core::{
  errors::ButtplugDeviceError,
  message::{InputReadingV4, InputType, InputTypeReading, InputValue},
  util::stream::convert_broadcast_receiver_to_stream,
};
use buttplug_server_device_config::{Endpoint, ServerDeviceDefinition};

const PRESSURE_SINGLE_FEATURE_INDEX: u32 = 2;
const PRESSURE_LEFT_FEATURE_INDEX: u32 = 2;
const PRESSURE_RIGHT_FEATURE_INDEX: u32 = 3;

pub type MagicMotionPressureEventStream =
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

pub struct MagicMotionPressureSubscriptions {
  // Set of pressure sensors we've subscribed to for updates.
  subscribed_sensors: Arc<AtomicU8>,
  event_stream: broadcast::Sender<ButtplugServerDeviceMessage>,
  format: MagicMotionPressureFormat,
}

impl MagicMotionPressureSubscriptions {
  pub fn new(format: MagicMotionPressureFormat) -> Self {
    let (sender, _) = broadcast::channel(256);
    Self {
      subscribed_sensors: Arc::new(AtomicU8::new(0)),
      event_stream: sender,
      format,
    }
  }

  pub fn subscribe(
    &self,
    device_index: u32,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
    _sensor_type: InputType,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    let sensor_bit = 1u8 << feature_index;
    let previous = self
      .subscribed_sensors
      .fetch_or(sensor_bit, Ordering::AcqRel);
    if previous & sensor_bit != 0 {
      debug!("Magic Motion already subscribed, skipping");
      return future::ready(Ok(())).boxed();
    }

    async move {
      // Only subscribe to the hardware endpoint once, no matter how many pressure features the
      // client subscribes to.
      if previous == 0
        && let Err(err) = device
          .subscribe(&HardwareSubscribeCmd::new(feature_id, Endpoint::RxPressure))
          .await
      {
        self
          .subscribed_sensors
          .fetch_and(!sensor_bit, Ordering::AcqRel);
        return Err(err);
      }

      if previous == 0 {
        let sender = self.event_stream.clone();
        let mut hardware_stream = device.event_stream();
        let stream_sensors = self.subscribed_sensors.clone();
        let format = self.format;
        debug!("Starting Magic Motion pressure subscription");
        buttplug_core::spawn!("Magic Motion pressure event handler", async move {
          // Raw u16 values are non-negative, so -1 can be used as a sentinel.
          let mut cached_values = [-1i32; 2];
          loop {
            let info = match hardware_stream.recv().await {
              Ok(info) => info,
              Err(RecvError::Lagged(n)) => {
                warn!("Magic Motion pressure subscription lagged, dropped {n} messages");
                continue;
              }
              Err(RecvError::Closed) => break,
            };
            let subscribed_sensors = stream_sensors.load(Ordering::Relaxed);
            if sender.receiver_count() == 0 || subscribed_sensors == 0 {
              return;
            }
            let HardwareEvent::Notification(_, endpoint, data) = info else {
              continue;
            };
            if endpoint != Endpoint::RxPressure {
              continue;
            }

            let (count, readings) = decode_pressure_data(format, &data);
            for (feature_index, value) in readings.into_iter().take(count) {
              if !send_pressure_reading(
                &sender,
                subscribed_sensors,
                device_index,
                feature_index,
                value,
                &mut cached_values[feature_index as usize - PRESSURE_SINGLE_FEATURE_INDEX as usize],
              ) {
                debug!("Hardware device listener for Magic Motion shut down, returning from task");
                return;
              }
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
    _sensor_type: InputType,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    let sensor_bit = 1u8 << feature_index;
    let previous = self
      .subscribed_sensors
      .fetch_and(!sensor_bit, Ordering::AcqRel);
    if previous & sensor_bit == 0 {
      return future::ready(Ok(())).boxed();
    }

    async move {
      if previous == sensor_bit {
        device
          .unsubscribe(&HardwareUnsubscribeCmd::new(
            feature_id,
            Endpoint::RxPressure,
          ))
          .await?;
      }
      Ok(())
    }
    .boxed()
  }

  pub fn event_stream(&self) -> MagicMotionPressureEventStream {
    convert_broadcast_receiver_to_stream(self.event_stream.subscribe()).boxed()
  }
}

fn decode_pressure_data(
  format: MagicMotionPressureFormat,
  data: &[u8],
) -> (usize, [(u32, i32); 2]) {
  match format {
    MagicMotionPressureFormat::Dual => {
      if data.len() < 4 {
        error!("Magic Motion pressure data not expected length: {}", data.len());
        return (0, [(0, 0), (0, 0)]);
      }

      // Flamingo Max sends two little-endian u16 pressure readings. The app calls the bytes at
      // 0/1 right and the bytes at 2/3 left.
      let raw_right = u16::from_le_bytes([data[0], data[1]]) as i32;
      let raw_left = u16::from_le_bytes([data[2], data[3]]) as i32;
      (
        2,
        [
          (PRESSURE_LEFT_FEATURE_INDEX, raw_left),
          (PRESSURE_RIGHT_FEATURE_INDEX, raw_right),
        ],
      )
    }
    MagicMotionPressureFormat::Single => {
      if data.len() < 2 {
        error!("Magic Motion pressure data not expected length: {}", data.len());
        return (0, [(0, 0), (0, 0)]);
      }

      // Single-sensor devices send one little-endian u16 pressure reading. The app divides this
      // by 10 for type-2 devices before its own UI scaling; we expose the raw value instead.
      let raw = u16::from_le_bytes([data[0], data[1]]) as i32;
      (1, [(PRESSURE_SINGLE_FEATURE_INDEX, raw), (0, 0)])
    }
  }
}

fn send_pressure_reading(
  sender: &broadcast::Sender<ButtplugServerDeviceMessage>,
  subscribed_sensors: u8,
  device_index: u32,
  feature_index: u32,
  value: i32,
  cached_value: &mut i32,
) -> bool {
  if subscribed_sensors & (1u8 << feature_index) == 0 || *cached_value == value {
    return true;
  }
  if sender
    .send(
      InputReadingV4::new(
        device_index,
        feature_index,
        InputTypeReading::Pressure(InputValue::new(value)),
      )
      .into(),
    )
    .is_err()
  {
    return false;
  }
  *cached_value = value;
  true
}

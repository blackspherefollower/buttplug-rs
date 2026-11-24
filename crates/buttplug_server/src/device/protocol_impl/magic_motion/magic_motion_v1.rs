// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use async_trait::async_trait;
use futures::{
  FutureExt,
  future::{self, BoxFuture},
};
use std::sync::{
  Arc,
  atomic::{AtomicI32, Ordering},
};
use uuid::Uuid;

use crate::device::{
  hardware::{Hardware, HardwareCommand, HardwareReadCmd, HardwareWriteCmd},
  protocol::{ProtocolHandler, ProtocolIdentifier, ProtocolInitializer},
};
use buttplug_core::{
  errors::ButtplugDeviceError,
  message::{InputReadingV4, InputType, InputTypeReading, InputValue},
};
use buttplug_server_device_config::{
  Endpoint,
  ProtocolCommunicationSpecifier,
  ServerDeviceDefinition,
  UserDeviceIdentifier,
};

use super::magic_motion_input::{
  MagicMotionInputEventStream,
  MagicMotionInputSubscriptions,
  MagicMotionPressureFormat,
  accelerometer_subscriptions,
  decode_counter,
  pressure_subscriptions,
  select_event_streams,
};

pub mod setup {
  use crate::device::protocol::{ProtocolIdentifier, ProtocolIdentifierFactory};

  #[derive(Default)]
  pub struct MagicMotionV1IdentifierFactory {}

  impl ProtocolIdentifierFactory for MagicMotionV1IdentifierFactory {
    fn identifier(&self) -> &str {
      "magic-motion-1"
    }

    fn create(&self) -> Box<dyn ProtocolIdentifier> {
      Box::new(super::MagicMotionV1Identifier::default())
    }
  }
}

#[derive(Default)]
pub struct MagicMotionV1Identifier {}

#[async_trait]
impl ProtocolIdentifier for MagicMotionV1Identifier {
  async fn identify(
    &mut self,
    hardware: Arc<Hardware>,
    _: ProtocolCommunicationSpecifier,
  ) -> Result<(UserDeviceIdentifier, Box<dyn ProtocolInitializer>), ButtplugDeviceError> {
    let mut name = hardware.name().to_owned();
    if name == "Smart Bean" && !hardware.endpoints().contains(&Endpoint::RxPressure) {
      name = "Smart Bean (no-pressure)".to_owned();
    }
    Ok((
      UserDeviceIdentifier::new(hardware.address(), "magic-motion-1", &Some(name)),
      Box::new(MagicMotionV1Initializer::default()),
    ))
  }
}

#[derive(Default)]
pub struct MagicMotionV1Initializer {}

fn input_feature_indices(def: &ServerDeviceDefinition, input_type: InputType) -> Vec<u32> {
  def
    .features()
    .values()
    .filter(|feature| feature.contains_input(input_type))
    .map(|feature| feature.index())
    .collect()
}

#[async_trait]
impl ProtocolInitializer for MagicMotionV1Initializer {
  async fn initialize(
    &mut self,
    hardware: Arc<Hardware>,
    def: &ServerDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    let accelerometer_indices = input_feature_indices(def, InputType::Accelerometer);

    // The step counter is read-only, but the device only updates it after the step enable
    // characteristic has been written. Do this at connect time so the first read is already valid.
    // The G-sensor data characteristic is left alone; it is subscribed only when an accelerometer
    // feature is subscribed.
    if let Some(counter_feature) = def
      .features()
      .values()
      .find(|feature| feature.contains_input(InputType::Counter))
    {
      hardware
        .write_value(&HardwareWriteCmd::new(
          &[counter_feature.id()],
          Endpoint::Generic1,
          vec![0x01, 0x00],
          true,
        ))
        .await?;
    }

    Ok(Arc::new(MagicMotionV1::new(
      MagicMotionPressureFormat::for_device_definition(def),
      [
        accelerometer_indices.first().copied(),
        accelerometer_indices.get(1).copied(),
        accelerometer_indices.get(2).copied(),
      ],
    )))
  }
}

pub struct MagicMotionV1 {
  pressure: MagicMotionInputSubscriptions<2>,
  accelerometer: MagicMotionInputSubscriptions<3>,
  counter_total: AtomicI32,
}

impl MagicMotionV1 {
  fn new(format: MagicMotionPressureFormat, accelerometer_indices: [Option<u32>; 3]) -> Self {
    Self {
      pressure: pressure_subscriptions(format),
      accelerometer: accelerometer_subscriptions(accelerometer_indices),
      counter_total: AtomicI32::new(0),
    }
  }

  fn add_counter_delta(&self, delta: i32) -> i32 {
    loop {
      let current = self.counter_total.load(Ordering::Acquire);
      let updated = current.saturating_add(delta);
      if self
        .counter_total
        .compare_exchange_weak(current, updated, Ordering::AcqRel, Ordering::Relaxed)
        .is_ok()
      {
        break updated;
      }
    }
  }
}

impl ProtocolHandler for MagicMotionV1 {
  fn handle_output_vibrate_cmd(
    &self,
    _feature_index: u32,
    feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    Ok(vec![
      HardwareWriteCmd::new(
        &[feature_id],
        Endpoint::Tx,
        vec![
          0x0b,
          0xff,
          0x04,
          0x0a,
          0x32,
          0x32,
          0x00,
          0x04,
          0x08,
          speed as u8,
          0x64,
          0x00,
        ],
        true,
      )
      .into(),
    ])
  }

  fn handle_output_oscillate_cmd(
    &self,
    _feature_index: u32,
    feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    Ok(vec![
      HardwareWriteCmd::new(
        &[feature_id],
        Endpoint::Tx,
        vec![
          0x0b,
          0xff,
          0x04,
          0x0a,
          0x32,
          0x32,
          0x00,
          0x04,
          0x08,
          speed as u8,
          0x64,
          0x00,
        ],
        false,
      )
      .into(),
    ])
  }

  fn handle_input_subscribe_cmd(
    &self,
    device_index: u32,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
    sensor_type: InputType,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    match sensor_type {
      InputType::Pressure => {
        self
          .pressure
          .subscribe(device_index, device, feature_index, feature_id)
      }
      InputType::Accelerometer => {
        self
          .accelerometer
          .subscribe(device_index, device, feature_index, feature_id)
      }
      _ => future::ready(Err(ButtplugDeviceError::UnhandledCommand(format!(
        "Magic Motion V1 input subscribe not implemented for {sensor_type}"
      ))))
      .boxed(),
    }
  }

  fn handle_input_unsubscribe_cmd(
    &self,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
    sensor_type: InputType,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    match sensor_type {
      InputType::Pressure => self.pressure.unsubscribe(device, feature_index, feature_id),
      InputType::Accelerometer => self
        .accelerometer
        .unsubscribe(device, feature_index, feature_id),
      _ => future::ready(Err(ButtplugDeviceError::UnhandledCommand(format!(
        "Magic Motion V1 input unsubscribe not implemented for {sensor_type}"
      ))))
      .boxed(),
    }
  }

  fn handle_input_read_cmd(
    &self,
    device_index: u32,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
    sensor_type: InputType,
  ) -> BoxFuture<'_, Result<InputReadingV4, ButtplugDeviceError>> {
    match sensor_type {
      InputType::Battery => {
        self.handle_battery_level_cmd(device_index, device, feature_index, feature_id)
      }
      InputType::Counter => async move {
        // The step enable characteristic is written during initialization. Reading this
        // characteristic returns the steps accumulated since the last read and clears the
        // device-side counter.
        let hw_msg = device
          .read_value(&HardwareReadCmd::new(feature_id, Endpoint::Generic0, 4, 0))
          .await?;
        let Some(values) = decode_counter(hw_msg.data()) else {
          return Err(ButtplugDeviceError::UnhandledCommand(
            "Magic Motion counter data not expected length".to_string(),
          ));
        };
        // The device reports a delta and clears it on read. Accumulate it so clients see a
        // cumulative step count instead of a value that drops back to zero after each read.
        let total = self.add_counter_delta(values[0]);
        Ok(InputReadingV4::new(
          device_index,
          feature_index,
          InputTypeReading::Counter(InputValue::new(total)),
        ))
      }
      .boxed(),
      _ => future::ready(Err(ButtplugDeviceError::UnhandledCommand(format!(
        "Magic Motion V1 input read not implemented for {sensor_type}"
      ))))
      .boxed(),
    }
  }

  fn event_stream(&self) -> MagicMotionInputEventStream {
    select_event_streams(vec![
      self.pressure.event_stream(),
      self.accelerometer.event_stream(),
    ])
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_counter_totals_are_cumulative() {
    let protocol = MagicMotionV1::new(MagicMotionPressureFormat::Single, [None, None, None]);
    assert_eq!(protocol.add_counter_delta(5), 5);
    assert_eq!(protocol.add_counter_delta(7), 12);
    assert_eq!(protocol.add_counter_delta(0), 12);
  }

  #[test]
  fn test_counter_total_saturates() {
    let protocol = MagicMotionV1::new(MagicMotionPressureFormat::Single, [None, None, None]);
    assert_eq!(protocol.add_counter_delta(i32::MAX), i32::MAX);
    assert_eq!(protocol.add_counter_delta(1), i32::MAX);
  }
}

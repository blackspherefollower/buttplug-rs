// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use futures::future::BoxFuture;
use std::sync::Arc;
use uuid::Uuid;

use crate::device::{
  hardware::{Hardware, HardwareCommand, HardwareWriteCmd},
  protocol::{ProtocolHandler, generic_protocol_setup},
};
use buttplug_core::{errors::ButtplugDeviceError, message::InputType};
use buttplug_server_device_config::Endpoint;

use super::magic_motion_pressure::{
  MagicMotionPressureEventStream,
  MagicMotionPressureFormat,
  MagicMotionPressureSubscriptions,
};

generic_protocol_setup!(MagicMotionV3, "magic-motion-3");

pub struct MagicMotionV3 {
  pressure: MagicMotionPressureSubscriptions,
}

impl Default for MagicMotionV3 {
  fn default() -> Self {
    Self {
      pressure: MagicMotionPressureSubscriptions::new(MagicMotionPressureFormat::Single),
    }
  }
}

impl ProtocolHandler for MagicMotionV3 {
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
          0x0b, 0xff, 0x04, 0x0a, 0x46, 0x46, 0x00, 0x04, 0x08, speed as u8, 0x64, 0x00,
        ],
        true,
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
    self
      .pressure
      .subscribe(device_index, device, feature_index, feature_id, sensor_type)
  }

  fn handle_input_unsubscribe_cmd(
    &self,
    device: Arc<Hardware>,
    feature_index: u32,
    feature_id: Uuid,
    sensor_type: InputType,
  ) -> BoxFuture<'_, Result<(), ButtplugDeviceError>> {
    self
      .pressure
      .unsubscribe(device, feature_index, feature_id, sensor_type)
  }

  fn event_stream(&self) -> MagicMotionPressureEventStream {
    self.pressure.event_stream()
  }
}

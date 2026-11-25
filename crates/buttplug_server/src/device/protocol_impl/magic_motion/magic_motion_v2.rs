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
  atomic::{AtomicU8, Ordering},
};
use uuid::{Uuid, uuid};

use crate::device::{
  hardware::{Hardware, HardwareCommand, HardwareWriteCmd},
  protocol::{
    ProtocolHandler,
    ProtocolIdentifier,
    ProtocolInitializer,
    generic_protocol_initializer_setup,
  },
};
use buttplug_core::{errors::ButtplugDeviceError, message::InputType};
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
  pressure_subscriptions,
};

const MAGIC_MOTION_2_PROTOCOL_UUID: Uuid = uuid!("4d6e9297-c57e-4ce7-a63c-24cc7d117a47");

generic_protocol_initializer_setup!(MagicMotionV2, "magic-motion-2");

#[derive(Default)]
pub struct MagicMotionV2Initializer {}

#[async_trait]
impl ProtocolInitializer for MagicMotionV2Initializer {
  async fn initialize(
    &mut self,
    _: Arc<Hardware>,
    def: &ServerDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    Ok(Arc::new(MagicMotionV2::new(
      MagicMotionPressureFormat::for_device_definition(def),
    )))
  }
}

pub struct MagicMotionV2 {
  speeds: [AtomicU8; 2],
  pressure: MagicMotionInputSubscriptions<2>,
}

impl MagicMotionV2 {
  fn new(format: MagicMotionPressureFormat) -> Self {
    Self {
      speeds: [AtomicU8::new(0), AtomicU8::new(0)],
      pressure: pressure_subscriptions(format),
    }
  }

  fn handle_output_cmd(
    &self,
    feature_index: u32,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    self.speeds[feature_index as usize].store(speed as u8, Ordering::Relaxed);
    let data = vec![
      0x10,
      0xff,
      0x04,
      0x0a,
      0x32,
      0x0a,
      0x00,
      0x04,
      0x08,
      self.speeds[0].load(Ordering::Relaxed),
      0x64,
      0x00,
      0x04,
      0x08,
      self.speeds[1].load(Ordering::Relaxed),
      0x64,
      0x01,
    ];
    Ok(vec![
      HardwareWriteCmd::new(&[MAGIC_MOTION_2_PROTOCOL_UUID], Endpoint::Tx, data, false).into(),
    ])
  }
}

impl ProtocolHandler for MagicMotionV2 {
  fn handle_output_vibrate_cmd(
    &self,
    feature_index: u32,
    _feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    self.handle_output_cmd(feature_index, speed)
  }
  fn handle_output_oscillate_cmd(
    &self,
    feature_index: u32,
    _feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    self.handle_output_cmd(feature_index, speed)
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
      _ => future::ready(Err(ButtplugDeviceError::UnhandledCommand(format!(
        "Magic Motion V2 input subscribe not implemented for {sensor_type}"
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
      _ => future::ready(Err(ButtplugDeviceError::UnhandledCommand(format!(
        "Magic Motion V2 input unsubscribe not implemented for {sensor_type}"
      ))))
      .boxed(),
    }
  }

  fn event_stream(&self) -> MagicMotionInputEventStream {
    self.pressure.event_stream()
  }
}

// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use std::sync::{
  Arc,
  atomic::{AtomicU8, Ordering},
};

use async_trait::async_trait;
use futures::{
  FutureExt,
  future::{self, BoxFuture},
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
use buttplug_server_device_config::Endpoint;
use buttplug_server_device_config::ServerDeviceDefinition;
use buttplug_server_device_config::{ProtocolCommunicationSpecifier, UserDeviceIdentifier};

use super::magic_motion_input::{
  MagicMotionInputEventStream,
  MagicMotionInputSubscriptions,
  MagicMotionPressureFormat,
  pressure_subscriptions,
};

const MAGICMOTIONV4_PROTOCOL_UUID: Uuid = uuid!("d4d62d09-c3e1-44c9-8eba-caa15de5b2a7");

generic_protocol_initializer_setup!(MagicMotionV4, "magic-motion-4");

#[derive(Default)]
pub struct MagicMotionV4Initializer {}

#[async_trait]
impl ProtocolInitializer for MagicMotionV4Initializer {
  async fn initialize(
    &mut self,
    _: Arc<Hardware>,
    def: &ServerDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    Ok(Arc::new(MagicMotionV4::new(
      def.features().values().filter(|x| x.has_output()).count() as u8,
    )))
  }
}

pub struct MagicMotionV4 {
  current_commands: Vec<AtomicU8>,
  pressure: MagicMotionInputSubscriptions<2>,
}

impl MagicMotionV4 {
  fn new(num_vibrators: u8) -> Self {
    Self {
      current_commands: std::iter::repeat_with(AtomicU8::default)
        .take(num_vibrators as usize)
        .collect(),
      pressure: pressure_subscriptions(MagicMotionPressureFormat::Single),
    }
  }
}

impl ProtocolHandler for MagicMotionV4 {
  fn handle_output_vibrate_cmd(
    &self,
    feature_index: u32,
    _feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    let data = if self.current_commands.len() == 1 {
      vec![
        0x10,
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
        0x04,
        0x08,
        speed as u8,
        0x64,
        0x01,
      ]
    } else {
      self.current_commands[feature_index as usize].store(speed as u8, Ordering::Relaxed);
      let speed0 = self.current_commands[0].load(Ordering::Relaxed);
      let speed1 = self.current_commands[1].load(Ordering::Relaxed);
      vec![
        0x10, 0xff, 0x04, 0x0a, 0x32, 0x32, 0x00, 0x04, 0x08, speed0, 0x64, 0x00, 0x04, 0x08,
        speed1, 0x64, 0x01,
      ]
    };
    Ok(vec![
      HardwareWriteCmd::new(&[MAGICMOTIONV4_PROTOCOL_UUID], Endpoint::Tx, data, true).into(),
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
      _ => future::ready(Err(ButtplugDeviceError::UnhandledCommand(format!(
        "Magic Motion V4 input subscribe not implemented for {sensor_type}"
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
        "Magic Motion V4 input unsubscribe not implemented for {sensor_type}"
      ))))
      .boxed(),
    }
  }

  fn event_stream(&self) -> MagicMotionInputEventStream {
    self.pressure.event_stream()
  }
}

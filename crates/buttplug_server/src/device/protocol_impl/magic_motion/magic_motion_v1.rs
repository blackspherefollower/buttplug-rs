// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use async_trait::async_trait;
use futures::future::BoxFuture;
use std::sync::Arc;
use uuid::Uuid;

use crate::device::{
  hardware::{Hardware, HardwareCommand, HardwareWriteCmd},
  protocol::{
    ProtocolHandler,
    ProtocolIdentifier,
    ProtocolInitializer
  },
};
use buttplug_core::{errors::ButtplugDeviceError, message::InputType};
use buttplug_server_device_config::{
  Endpoint,
  ProtocolCommunicationSpecifier,
  ServerDeviceDefinition,
  UserDeviceIdentifier,
};

use super::magic_motion_pressure::{
  MagicMotionPressureEventStream,
  MagicMotionPressureFormat,
  MagicMotionPressureSubscriptions,
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
Ok((UserDeviceIdentifier::new(hardware.address(), "magic-motion-1", &Some(name)), Box::new(MagicMotionV1Initializer::default())))
}
}


#[derive(Default)]
pub struct MagicMotionV1Initializer {}

#[async_trait]
impl ProtocolInitializer for MagicMotionV1Initializer {
  async fn initialize(
    &mut self,
    _: Arc<Hardware>,
    def: &ServerDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    Ok(Arc::new(MagicMotionV1::new(
      MagicMotionPressureFormat::for_device_definition(def),
    )))
  }
}

pub struct MagicMotionV1 {
  pressure: MagicMotionPressureSubscriptions,
}

impl MagicMotionV1 {
  fn new(format: MagicMotionPressureFormat) -> Self {
    Self {
      pressure: MagicMotionPressureSubscriptions::new(format),
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
          0x0b, 0xff, 0x04, 0x0a, 0x32, 0x32, 0x00, 0x04, 0x08, speed as u8, 0x64, 0x00,
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
          0x0b, 0xff, 0x04, 0x0a, 0x32, 0x32, 0x00, 0x04, 0x08, speed as u8, 0x64, 0x00,
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

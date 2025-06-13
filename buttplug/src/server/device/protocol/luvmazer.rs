// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2025 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use crate::{
  core::{
    errors::ButtplugDeviceError,
    message::{ActuatorType, Endpoint},
  },
  server::device::{
    configuration::UserDeviceDefinition,
    hardware::{Hardware, HardwareCommand, HardwareWriteCmd},
    protocol::{
      generic_protocol_initializer_setup,
      ProtocolCommunicationSpecifier,
      ProtocolHandler,
      ProtocolIdentifier,
      ProtocolInitializer,
      UserDeviceIdentifier,
    },
  },
  util::async_manager,
};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;

generic_protocol_initializer_setup!(Luvmazer, "luvmazer");

async fn delayed_handler(device: Arc<Hardware>, cmd: u8, mode: u8, idx:u8, scalar: u8) {
  sleep(Duration::from_millis(50)).await;
  let res = device
    .write_value(&HardwareWriteCmd::new(
      Endpoint::Tx,
      vec![0xa0, cmd, mode, idx, 0x64, scalar as u8],
      false,
    ))
    .await;
  if res.is_err() {
    error!("Delayed Luvmazer command error: {:?}", res.err());
  }
}

async fn delayed_handler_long(device: Arc<Hardware>, cmd: u8, mode: u8, idx:u8, bits:u8, scalar: u8) {
  sleep(Duration::from_millis(100)).await;
  let res = device
      .write_value(&HardwareWriteCmd::new(
        Endpoint::Tx,
        vec![0xa0, cmd, mode, idx, bits, scalar as u8],
        false,
      ))
      .await;
  if res.is_err() {
    error!("Delayed Luvmazer command error: {:?}", res.err());
  }
}

#[derive(Default)]
pub struct LuvmazerInitializer {}

#[async_trait]
impl ProtocolInitializer for LuvmazerInitializer {
  async fn initialize(
    &mut self,
    hardware: Arc<Hardware>,
    _: &UserDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    Ok(Arc::new(Luvmazer::new(hardware)))
  }
}

pub struct Luvmazer {
  device: Arc<Hardware>,
}

impl Luvmazer {
  fn new(device: Arc<Hardware>) -> Self {
    Self { device }
  }
}

impl ProtocolHandler for Luvmazer {
  fn keepalive_strategy(&self) -> super::ProtocolKeepaliveStrategy {
    super::ProtocolKeepaliveStrategy::RepeatLastPacketStrategy
  }

  fn handle_scalar_cmd(
    &self,
    commands: &[Option<(ActuatorType, u32)>],
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    let cmd1 = commands[0];
    let cmd2 = if commands.len() > 1 {
      commands[1]
    } else {
      None
    };
    let cmd3 = if commands.len() > 2 {
      commands[2]
    } else {
      None
    };
    
    if let Some(cmd) = cmd2 {
      if cmd.0 == ActuatorType::Rotate {
        if cmd1.is_some() {
          let dev = self.device.clone();
          async_manager::spawn(async move { delayed_handler(dev, 0x0f,0x00, 0x00, cmd.1 as u8).await });
        } else {
          return Ok(vec![HardwareWriteCmd::new(
            Endpoint::Tx,
            vec![0xa0, 0x0f, 0x00, 0x00, 0x64, cmd.1 as u8],
            false,
          )
          .into()]);
        }
      } else if cmd.0 == ActuatorType::Oscillate {
        if cmd1.is_some() {
          let dev = self.device.clone();
          async_manager::spawn(async move { delayed_handler(dev, 0x06, 0x01, 0x00, cmd.1 as u8).await });
        } else {
          return Ok(vec![HardwareWriteCmd::new(
            Endpoint::Tx,
            vec![0xa0, 0x06, 0x01, 0x00, 0x64, cmd.1 as u8],
            false,
          )
              .into()]);
        }
      } else if cmd.0 == ActuatorType::Vibrate {
        if cmd1.is_some() {
          let dev = self.device.clone();
          async_manager::spawn(async move { delayed_handler(dev, 0x01, 0x00, 0x01, cmd.1 as u8).await });
        } else {
          return Ok(vec![HardwareWriteCmd::new(
            Endpoint::Tx,
            vec![0xa0, 0x01, 0x00, 0x01, 0x64, cmd.1 as u8],
            false,
          )
              .into()]);
        }
      }
    }


    if let Some(cmd) = cmd3 {
      if cmd.0 == ActuatorType::Constrict {
          if cmd1.is_some() {
            let dev = self.device.clone();
            async_manager::spawn(async move { delayed_handler_long(dev, 0x01, 0x00, 0x01, if cmd.1 == 0 {0x00} else {0x64}, cmd.1 as u8).await });
          } else {
            return Ok(vec![HardwareWriteCmd::new(
              Endpoint::Tx,
              vec![0xa0, 0x0d, 0x00, 0x00, if cmd.1 == 0 {0x00} else {0x64}, cmd.1 as u8],
              false,
            )
                .into()]);
          }
      }
    }
      
    /*
    vec![0xa0, 0x0d, 0x00, 0x00, 0x64, 0xff], # inflate
    vec![0xa0, 0x0d, 0x00, 0x00, 0x00, 0x00], # deflate
     */

    let idx = 0;
    if let Some(cmd) = cmd1 {
      return Ok(vec![HardwareWriteCmd::new(
        Endpoint::Tx,
        vec![0xa0, 0x01, 0x00, idx, 0x64, cmd.1 as u8],
        false,
      )
      .into()]);
    }

    Ok(vec![])
  }
}

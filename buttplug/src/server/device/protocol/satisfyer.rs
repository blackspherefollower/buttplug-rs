// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2022 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use super::{ButtplugProtocol, ButtplugProtocolFactory, ButtplugProtocolCommandHandler, ButtplugDeviceMessageType::ScalarCmd};
use crate::{
  core::messages::{self, ButtplugDeviceCommandMessageUnion, Endpoint},
  server::{
    ButtplugServerResultFuture,
    device::{
      protocol::{generic_command_manager::GenericCommandManager, ButtplugProtocolProperties},
      configuration::{ProtocolDeviceAttributes, ProtocolDeviceAttributesBuilder, ProtocolAttributesIdentifier},
      hardware::{Hardware, HardwareReadCmd, HardwareWriteCmd},
    },
  },
  util::async_manager,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;


pub struct Satisfyer {
  device_attributes: ProtocolDeviceAttributes,
  manager: Arc<Mutex<GenericCommandManager>>,
  stop_commands: Vec<ButtplugDeviceCommandMessageUnion>,
  last_command: Arc<Mutex<Vec<u8>>>,
}

// Satisfyer toys will drop their connections if they don't get an update within ~10 seconds.
// Therefore we try to send a command every ~3s unless something is sent/updated sooner.
async fn send_satisfyer_updates(device: Arc<Hardware>, data: Arc<Mutex<Vec<u8>>>) {
  while device.connected() {
    // Scope to make sure we drop the lock before sleeping.
    {
      let current_data = data.lock().await.clone();
      if let Err(e) = device
        .write_value(HardwareWriteCmd::new(
          Endpoint::Tx,
          current_data.clone().to_vec(),
          false,
        ))
        .await
      {
        error!(
          "Got an error from a satisfyer device, exiting control loop: {:?}",
          e
        );
        break;
      }
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
  }
}

impl Satisfyer {
  const PROTOCOL_IDENTIFIER: &'static str = "satisfyer";

  fn new(
    device_attributes: ProtocolDeviceAttributes,
    last_command: Arc<Mutex<Vec<u8>>>,
  ) -> Self {
    let manager = GenericCommandManager::new(&device_attributes);
    Self {
      device_attributes,
      stop_commands: manager.stop_commands(),
      manager: Arc::new(Mutex::new(manager)),
      last_command,
    }
  }
}

#[derive(Default, Debug)]
pub struct SatisfyerFactory {}

impl ButtplugProtocolFactory for SatisfyerFactory {
  fn try_create(
    &self,
    hardware: Arc<Hardware>,
    builder: ProtocolDeviceAttributesBuilder,
  ) -> futures::future::BoxFuture<
    'static,
    Result<Box<dyn ButtplugProtocol>, crate::core::errors::ButtplugError>,
  > {
    let msg = HardwareWriteCmd::new(Endpoint::Command, vec![0x01], true);
    let info_fut = hardware.write_value(msg);

    async move {
      let result = hardware
        .read_value(HardwareReadCmd::new(Endpoint::RxBLEModel, 128, 500))
        .await?;
      let device_identifier = format!(
        "{}",
        u32::from_be_bytes(result.data().to_vec().try_into().unwrap_or([0; 4]))
      );
      info!(
        "Satisfyer Device Identifier: {:?} {}",
        result.data(),
        device_identifier
      );
      info_fut.await?;
      let device_attributes = builder.create(hardware.address(), &ProtocolAttributesIdentifier::Identifier(device_identifier), &hardware.endpoints())?;

      // Now that we've initialized and constructed the device, start the update cycle to make sure
      // we don't drop the connection.
      let mut size: usize = 8;
      if let Some(vattr) = device_attributes.get(&ScalarCmd) {
        if let Some(count) = vattr.feature_count {
          size = 4 * count as usize;
        }
      }
      let last_command = Arc::new(Mutex::new(vec![0u8; size]));
      let device = Satisfyer::new(device_attributes, last_command.clone());
      async_manager::spawn(async move {
        send_satisfyer_updates(hardware, last_command).await;
      });
      Ok(Arc::new(device) as Box<dyn ButtplugProtocol>)
    }.boxed()
  }

  fn protocol_identifier(&self) -> &'static str {
    Satisfyer::PROTOCOL_IDENTIFIER
  }
}

impl ButtplugProtocol for Satisfyer {}

crate::default_protocol_properties_definition!(Satisfyer);

impl ProtocolHandler for Satisfyer {
  fn handle_vibrate_cmd(
    &self,
    cmds: &Vec<Option<u32>>,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    // Store off result before the match, so we drop the lock ASAP.
    let manager = self.manager.clone();
    let last_command = self.last_command.clone();
    async move {
      let result = manager.lock().await.update_vibration(&message, true)?;
      if let Some(cmds) = result {
        let mut data = vec![0u8; (cmds.len() * 4) as usize];
        for (i, cmd) in cmds.iter().enumerate() {
          for j in 0..4 {
            data[(i * 4) + j] = cmd.unwrap_or(0) as u8
          }
        }
        *last_command.lock().await = data.clone();
        device
          .write_value(HardwareWriteCmd::new(Endpoint::Tx, data, false))
          .await?;
      }
      Ok(messages::Ok::default().into())
    }.boxed()
  }
}

#[cfg(all(test, feature = "server"))]
mod test {
  use crate::{
    core::messages::{Endpoint, StopDeviceCmd, VibrateCmd, VibrateSubcommand},
    server::device::{
      hardware::{HardwareCommand, HardwareWriteCmd},
      hardware::communication::test::{
        check_test_recv_empty,
        check_test_recv_value,
        new_bluetoothle_test_device,
      },
    },
    util::async_manager,
  };

  #[test]
  #[ignore = "Reimplement with name readout and timing fixes (#414)"]
  pub fn test_satisfyer_2v_protocol() {
    async_manager::block_on(async move {
      let (device, test_device) = new_bluetoothle_test_device("SF Curvy 2+")
        .await
        .expect("Test, assuming infallible");
      let command_receiver = test_device
        .endpoint_receiver(&Endpoint::Tx)
        .expect("Test, assuming infallible");
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(0, 0.5)]).into())
        .await
        .expect("Test, assuming infallible");
      /*
      check_test_recv_value(
        &command_receiver,
        HardwareCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0, 0, 0, 0, 0, 0, 0, 0],
          false,
        )),
      );
       */
      check_test_recv_value(
        &command_receiver,
        HardwareCommand::Write(HardwareWriteCmd::new(
          Endpoint::Tx,
          vec![0, 0, 0, 0, 50, 50, 50, 50],
          false,
        )),
      );
      // Since we only created one subcommand, we should only receive one command.
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(0, 0.5)]).into())
        .await
        .expect("Test, assuming infallible");
      assert!(check_test_recv_empty(&command_receiver));
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(1, 0.9)]).into())
        .await
        .expect("Test, assuming infallible");
      check_test_recv_value(
        &command_receiver,
        HardwareCommand::Write(HardwareWriteCmd::new(
          Endpoint::Tx,
          vec![90, 90, 90, 90, 50, 50, 50, 50],
          false,
        )),
      );
      device
        .parse_message(StopDeviceCmd::new(0).into())
        .await
        .expect("Test, assuming infallible");
      check_test_recv_value(
        &command_receiver,
        HardwareCommand::Write(HardwareWriteCmd::new(
          Endpoint::Tx,
          vec![0, 0, 0, 0, 0, 0, 0, 0],
          false,
        )),
      );
    });
  }

  #[test]
  #[ignore = "Reimplement with name readout and timing fixes (#414)"]
  pub fn test_satisfyer_1v_protocol() {
    async_manager::block_on(async move {
      let (device, test_device) = new_bluetoothle_test_device("SF Royal One")
        .await
        .expect("Test, assuming infallible");
      let command_receiver = test_device
        .endpoint_receiver(&Endpoint::Tx)
        .expect("Test, assuming infallible");
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(0, 0.5)]).into())
        .await
        .expect("Test, assuming infallible");
      check_test_recv_value(
        &command_receiver,
        HardwareCommand::Write(HardwareWriteCmd::new(
          Endpoint::Tx,
          vec![0, 0, 0, 0, 50, 50, 50, 50],
          false,
        )),
      );
      // Since we only created one subcommand, we should only receive one command.
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(0, 0.5)]).into())
        .await
        .expect("Test, assuming infallible");
      assert!(check_test_recv_empty(&command_receiver));
      device
        .parse_message(StopDeviceCmd::new(0).into())
        .await
        .expect("Test, assuming infallible");
      check_test_recv_value(
        &command_receiver,
        HardwareCommand::Write(HardwareWriteCmd::new(
          Endpoint::Tx,
          vec![0, 0, 0, 0, 0, 0, 0, 0],
          false,
        )),
      );
    });
  }
}

use super::{ButtplugDeviceResultFuture, ButtplugProtocol, ButtplugProtocolCommandHandler};
use crate::{
  core::messages::{
    self, ButtplugDeviceCommandMessageUnion, ButtplugDeviceMessage, DeviceMessageAttributesMap,
  },
  device::{
    protocol::{generic_command_manager::GenericCommandManager, ButtplugProtocolProperties},
    DeviceImpl, DeviceWriteCmd, Endpoint,
  },
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(ButtplugProtocolProperties)]
pub struct VorzeSA {
  name: String,
  message_attributes: DeviceMessageAttributesMap,
  manager: Arc<Mutex<GenericCommandManager>>,
  stop_commands: Vec<ButtplugDeviceCommandMessageUnion>,
}

impl ButtplugProtocol for VorzeSA {
  fn new_protocol(
    name: &str,
    message_attributes: DeviceMessageAttributesMap,
  ) -> Box<dyn ButtplugProtocol> {
    let manager = GenericCommandManager::new(&message_attributes);

    Box::new(Self {
      name: name.to_owned(),
      message_attributes,
      stop_commands: manager.get_stop_commands(),
      manager: Arc::new(Mutex::new(manager)),
    })
  }
}

#[repr(u8)]
#[derive(PartialEq)]
enum VorzeDevices {
  Bach = 6,
  Cyclone = 1,
  Rocket = 7,
  UFO = 2,
}

#[repr(u8)]
enum VorzeActions {
  Rotate = 1,
  Vibrate = 3,
}

impl ButtplugProtocolCommandHandler for VorzeSA {
  fn handle_vibrate_cmd(
    &self,
    device: Arc<DeviceImpl>,
    msg: messages::VibrateCmd,
  ) -> ButtplugDeviceResultFuture {
    let manager = self.manager.clone();
    let dev_id = if self.name.to_ascii_lowercase().contains("rocket") {
      VorzeDevices::Rocket
    } else {
      VorzeDevices::Bach
    };
    Box::pin(async move {
      let result = manager.lock().await.update_vibration(&msg, false)?;
      let mut fut_vec = vec![];
      if let Some(cmds) = result {
        if let Some(speed) = cmds[0] {
          let need_response = dev_id == VorzeDevices::Rocket;
          fut_vec.push(device.write_value(DeviceWriteCmd::new(
            Endpoint::Tx,
            vec![dev_id as u8, VorzeActions::Vibrate as u8, speed as u8],
            need_response,
          )));
        }
      }
      for fut in fut_vec {
        fut.await?;
      }
      Ok(messages::Ok::default().into())
    })
  }

  fn handle_rotate_cmd(
    &self,
    device: Arc<DeviceImpl>,
    msg: messages::RotateCmd,
  ) -> ButtplugDeviceResultFuture {
    let manager = self.manager.clone();
    // This will never change, so we can process it before the future.
    let dev_id = if self.name.contains("UFO") {
      VorzeDevices::UFO
    } else {
      VorzeDevices::Cyclone
    };
    Box::pin(async move {
      let result = manager.lock().await.update_rotation(&msg)?;
      let mut fut_vec = vec![];
      if let Some((speed, clockwise)) = result[0] {
        let data: u8 = (clockwise as u8) << 7 | (speed as u8);
        fut_vec.push(device.write_value(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![dev_id as u8, VorzeActions::Rotate as u8, data],
          false,
        )));
      }
      for fut in fut_vec {
        fut.await?;
      }
      Ok(messages::Ok::default().into())
    })
  }

  fn handle_vorze_a10_cyclone_cmd(
    &self,
    device: Arc<DeviceImpl>,
    msg: messages::VorzeA10CycloneCmd,
  ) -> ButtplugDeviceResultFuture {
    self.handle_rotate_cmd(
      device,
      messages::RotateCmd::new(
        msg.device_index(),
        vec![messages::RotationSubcommand::new(
          0,
          msg.speed() as f64 / 99f64,
          msg.clockwise(),
        )],
      ),
    )
  }
}

#[cfg(all(test, feature = "server"))]
mod test {
  use crate::{
    core::messages::{RotateCmd, RotationSubcommand, StopDeviceCmd, VibrateCmd, VibrateSubcommand},
    device::{DeviceImplCommand, DeviceWriteCmd, Endpoint},
    test::{check_test_recv_empty, check_test_recv_value, new_bluetoothle_test_device},
    util::async_manager,
  };

  #[test]
  pub fn test_vorze_sa_vibration_protocol_bach() {
    async_manager::block_on(async move {
      let (device, test_device) = new_bluetoothle_test_device("Bach smart").await.unwrap();
      let command_receiver = test_device.get_endpoint_receiver(&Endpoint::Tx).unwrap();
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(0, 0.5)]).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x06, 0x03, 50],
          false,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));

      device
        .parse_message(StopDeviceCmd::new(0).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x06, 0x03, 0x0],
          false,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));
    });
  }

  #[test]
  pub fn test_vorze_sa_vibration_protocol_rocket() {
    async_manager::block_on(async move {
      let (device, test_device) = new_bluetoothle_test_device("ROCKET").await.unwrap();
      let command_receiver = test_device.get_endpoint_receiver(&Endpoint::Tx).unwrap();
      device
        .parse_message(VibrateCmd::new(0, vec![VibrateSubcommand::new(0, 0.5)]).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x07, 0x03, 50],
          true,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));

      device
        .parse_message(StopDeviceCmd::new(0).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x07, 0x03, 0x0],
          true,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));
    });
  }

  #[test]
  pub fn test_vorze_sa_rotation_protocol() {
    async_manager::block_on(async move {
      let (device, test_device) = new_bluetoothle_test_device("CycSA").await.unwrap();
      let command_receiver = test_device.get_endpoint_receiver(&Endpoint::Tx).unwrap();
      device
        .parse_message(RotateCmd::new(0, vec![RotationSubcommand::new(0, 0.5, false)]).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x01, 0x01, 50],
          false,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));

      device
        .parse_message(RotateCmd::new(0, vec![RotationSubcommand::new(0, 0.5, true)]).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x01, 0x01, 178],
          false,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));

      device
        .parse_message(StopDeviceCmd::new(0).into())
        .await
        .unwrap();
      check_test_recv_value(
        &command_receiver,
        DeviceImplCommand::Write(DeviceWriteCmd::new(
          Endpoint::Tx,
          vec![0x01, 0x01, 0x0],
          false,
        )),
      );
      assert!(check_test_recv_empty(&command_receiver));
    });
  }
}

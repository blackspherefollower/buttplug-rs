use super::json::JSONValidator;
use crate::{
  core::{
    errors::{ButtplugDeviceError, ButtplugError},
    messages::DeviceMessageAttributesMap,
  },
  device::configuration_manager::{
    BluetoothLESpecifier, DeviceConfigurationManager, HIDSpecifier, LovenseConnectServiceSpecifier,
    ProtocolAttributeIdentifier, ProtocolDeviceAttributes, ProtocolDeviceConfiguration,
    ProtocolDeviceSpecifier, SerialSpecifier, USBSpecifier, WebsocketSpecifier, XInputSpecifier,
  },
};
use getset::{Getters, MutGetters, Setters};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

pub static DEVICE_CONFIGURATION_JSON: &str =
  include_str!("../../buttplug-device-config/buttplug-device-config.json");
static DEVICE_CONFIGURATION_JSON_SCHEMA: &str =
  include_str!("../../buttplug-device-config/buttplug-device-config-schema.json");

#[derive(Serialize, Deserialize, Debug, Getters, Setters, Default, Clone, PartialEq)]
#[getset(get = "pub", set = "pub")]
pub struct DeviceUserConfig {
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(default)]
  #[serde(rename = "display-name")]
  display_name: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(default)]
  allow: Option<bool>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(default)]
  deny: Option<bool>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(default)]
  messages: Option<DeviceMessageAttributesMap>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(default)]
  index: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ProtocolAttributes {
  #[serde(skip_serializing_if = "Option::is_none")]
  identifier: Option<Vec<String>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  name: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  messages: Option<DeviceMessageAttributesMap>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct UserConfigAttributes {
  #[serde(skip_serializing_if = "Option::is_none")]
  identifier: Option<Vec<String>>,
  #[serde(skip_serializing_if = "Option::is_none", rename = "user-configs")]
  user_configs: Option<HashMap<String, DeviceUserConfig>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ProtocolDefinition {
  // Can't get serde flatten specifiers into a String/DeviceSpecifier map, so
  // they're kept separate here, and we return them in get_specifiers(). Feels
  // very clumsy, but we really don't do this a bunch during a session.
  #[serde(skip_serializing_if = "Option::is_none")]
  usb: Option<Vec<USBSpecifier>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  btle: Option<BluetoothLESpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  serial: Option<Vec<SerialSpecifier>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  hid: Option<Vec<HIDSpecifier>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  xinput: Option<XInputSpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  websocket: Option<WebsocketSpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(rename = "lovense-connect-service")]
  lovense_connect_service: Option<LovenseConnectServiceSpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  defaults: Option<ProtocolAttributes>,
  #[serde(default)]
  configurations: Vec<ProtocolAttributes>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct UserConfigDefinition {
  // Can't get serde flatten specifiers into a String/DeviceSpecifier map, so
  // they're kept separate here, and we return them in get_specifiers(). Feels
  // very clumsy, but we really don't do this a bunch during a session.
  #[serde(skip_serializing_if = "Option::is_none")]
  usb: Option<Vec<USBSpecifier>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  btle: Option<BluetoothLESpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  serial: Option<Vec<SerialSpecifier>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  hid: Option<Vec<HIDSpecifier>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  xinput: Option<XInputSpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  websocket: Option<WebsocketSpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(rename = "lovense-connect-service")]
  lovense_connect_service: Option<LovenseConnectServiceSpecifier>,
  #[serde(skip_serializing_if = "Option::is_none")]
  defaults: Option<UserConfigAttributes>,
  #[serde(default)]
  configurations: HashMap<String, HashMap<String, DeviceUserConfig>>,
}

impl From<ProtocolDefinition> for ProtocolDeviceConfiguration {
  fn from(protocol_def: ProtocolDefinition) -> Self {
    // Make a vector out of the protocol definition specifiers
    let mut specifiers = vec![];
    if let Some(usb_vec) = protocol_def.usb {
      usb_vec
        .iter()
        .for_each(|spec| specifiers.push(ProtocolDeviceSpecifier::USB(*spec)));
    }
    if let Some(serial_vec) = protocol_def.serial {
      serial_vec
        .iter()
        .for_each(|spec| specifiers.push(ProtocolDeviceSpecifier::Serial(spec.clone())));
    }
    if let Some(hid_vec) = protocol_def.hid {
      hid_vec
        .iter()
        .for_each(|spec| specifiers.push(ProtocolDeviceSpecifier::HID(*spec)));
    }
    if let Some(btle) = protocol_def.btle {
      specifiers.push(ProtocolDeviceSpecifier::BluetoothLE(btle));
    }
    if let Some(xinput) = protocol_def.xinput {
      specifiers.push(ProtocolDeviceSpecifier::XInput(xinput));
    }
    if let Some(websocket) = protocol_def.websocket {
      specifiers.push(ProtocolDeviceSpecifier::Websocket(websocket));
    }
    if let Some(lcs) = protocol_def.lovense_connect_service {
      specifiers.push(ProtocolDeviceSpecifier::LovenseConnectService(lcs));
    }

    let mut configurations = HashMap::new();

    let default_attrs = if let Some(defaults) = protocol_def.defaults {
      let default_attrs = Arc::new(ProtocolDeviceAttributes::new(
        defaults.name,
        None,
        defaults.messages.unwrap_or_default(),
        None,
      ));
      configurations.insert(ProtocolAttributeIdentifier::Default, default_attrs.clone());
      Some(default_attrs)
    } else {
      None
    };

    for config in protocol_def.configurations {
      let config_attrs = Arc::new(ProtocolDeviceAttributes::new(
        config.name,
        None,
        config.messages.unwrap_or_default(),
        default_attrs.clone(),
      ));
      if let Some(identifiers) = config.identifier {
        for identifier in identifiers {
          configurations.insert(
            ProtocolAttributeIdentifier::Identifier(identifier),
            config_attrs.clone(),
          );
        }
      }
    }

    Self::new(specifiers, configurations)
  }
}

fn add_user_configs_to_protocol(
  external_config: &mut ExternalDeviceConfiguration,
  user_config_def: HashMap<String, UserConfigDefinition>,
) {
  for (user_config_protocol, protocol_def) in user_config_def {
    if !external_config.protocol_configurations.contains_key(&user_config_protocol) {
      continue;
    }

    let base_protocol_def = external_config.protocol_configurations.get_mut(&user_config_protocol).unwrap();

    // Make a vector out of the protocol definition specifiers
    if let Some(usb_vec) = protocol_def.usb {
      usb_vec.iter().for_each(|spec| {
        base_protocol_def
          .specifiers_mut()
          .push(ProtocolDeviceSpecifier::USB(*spec))
      });
    }
    if let Some(serial_vec) = protocol_def.serial {
      serial_vec.iter().for_each(|spec| {
        base_protocol_def
          .specifiers_mut()
          .push(ProtocolDeviceSpecifier::Serial(spec.clone()))
      });
    }
    if let Some(hid_vec) = protocol_def.hid {
      hid_vec.iter().for_each(|spec| {
        base_protocol_def
          .specifiers_mut()
          .push(ProtocolDeviceSpecifier::HID(*spec))
      });
    }
    if let Some(btle) = protocol_def.btle {
      base_protocol_def
        .specifiers_mut()
        .push(ProtocolDeviceSpecifier::BluetoothLE(btle));
    }
    if let Some(xinput) = protocol_def.xinput {
      base_protocol_def
        .specifiers_mut()
        .push(ProtocolDeviceSpecifier::XInput(xinput));
    }
    if let Some(websocket) = protocol_def.websocket {
      base_protocol_def
        .specifiers_mut()
        .push(ProtocolDeviceSpecifier::Websocket(websocket));
    }
    if let Some(lcs) = protocol_def.lovense_connect_service {
      base_protocol_def
        .specifiers_mut()
        .push(ProtocolDeviceSpecifier::LovenseConnectService(lcs));
    }


    let mut configurations = HashMap::new();
    if let Some(defaults) = base_protocol_def
      .configurations()
      .get(&ProtocolAttributeIdentifier::Default)
    {
      if let Some(user_config_defaults) = protocol_def.defaults {
        if let Some(user_config_defaults_config) = user_config_defaults.user_configs {
          for (address, user_config) in user_config_defaults_config {
            let config_attrs = Arc::new(ProtocolDeviceAttributes::new(
              None,
              user_config.display_name,
              user_config.messages.unwrap_or_default(),
              Some(defaults.clone()),
            ));
            configurations.insert(ProtocolAttributeIdentifier::Address(address), config_attrs);
          }
        }
      }
    }

    for (identifier, user_configuration) in protocol_def.configurations {
      if let Some(parent) = base_protocol_def.configurations().get(&ProtocolAttributeIdentifier::Identifier(identifier)) {
        for (address, user_config) in user_configuration {
          if *user_config.allow().as_ref().unwrap_or(&false) {
            external_config.allow_list.push(address.clone());
          }
          if *user_config.deny().as_ref().unwrap_or(&false) {
            external_config.deny_list.push(address.clone());
          }
          if let Some(index) = user_config.index().as_ref() {
            external_config.reserved_indexes.insert(*index, address.clone());
          }
          let config_attrs = Arc::new(ProtocolDeviceAttributes::new(
            None,
            user_config.display_name,
            user_config.messages.unwrap_or_default(),
            Some(parent.clone()),
          ));
          configurations.insert(ProtocolAttributeIdentifier::Address(address.clone()), config_attrs);
        }
      }
    }
    base_protocol_def.configurations_mut().extend(configurations);
  }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ProtocolConfiguration {
  pub version: u32,
  #[serde(default)]
  pub protocols: Option<HashMap<String, ProtocolDefinition>>,
  #[serde(rename = "user-configs", default)]
  pub user_configs: Option<HashMap<String, UserConfigDefinition>>,
}

impl Default for ProtocolConfiguration {
  fn default() -> Self {
    Self {
      version: get_internal_config_version(),
      protocols: Some(HashMap::new()),
      user_configs: Some(HashMap::new()),
    }
  }
}

impl ProtocolConfiguration {
  pub fn to_json(&self) -> String {
    serde_json::to_string(self)
      .expect("All types below this are Serialize, so this should be infallible.")
  }
}

#[derive(Default, Debug, Getters)]
#[getset(get="pub")]
pub struct ExternalDeviceConfiguration {
  allow_list: Vec<String>,
  deny_list: Vec<String>,
  reserved_indexes: HashMap<u32, String>,
  protocol_configurations: HashMap<String, ProtocolDeviceConfiguration>
}

pub fn get_internal_config_version() -> u32 {
  let config: ProtocolConfiguration = serde_json::from_str(DEVICE_CONFIGURATION_JSON)
    .expect("If this fails, the whole library goes with it.");
  config.version
}

pub fn load_protocol_config_from_json(
  config_str: &str,
  skip_version_check: bool,
) -> Result<ProtocolConfiguration, ButtplugError> {
  let config_validator = JSONValidator::new(DEVICE_CONFIGURATION_JSON_SCHEMA);
  match config_validator.validate(config_str) {
    Ok(_) => match serde_json::from_str::<ProtocolConfiguration>(config_str) {
      Ok(protocol_config) => {
        let internal_config_version = get_internal_config_version();
        if !skip_version_check && protocol_config.version < internal_config_version {
          Err(ButtplugDeviceError::DeviceConfigurationFileError(format!(
            "Device configuration file version {} is older than internal version {}. Please use a newer file.",
            protocol_config.version,
            internal_config_version
          )).into())
        } else {
          Ok(protocol_config)
        }
      }
      Err(err) => Err(ButtplugDeviceError::DeviceConfigurationFileError(format!("{}", err)).into()),
    },
    Err(err) => Err(ButtplugDeviceError::DeviceConfigurationFileError(format!("{}", err)).into()),
  }
}

pub fn load_protocol_configs_from_json(
  main_config_str: Option<String>,
  user_config_str: Option<String>,
  skip_version_check: bool,
) -> Result<ExternalDeviceConfiguration, ButtplugError> {
  // Start by loading the main config
  let main_config = load_protocol_config_from_json(
    &main_config_str.unwrap_or(DEVICE_CONFIGURATION_JSON.to_owned()),
    skip_version_check,
  )?;

  // Each protocol will need to become a ProtocolDeviceConfiguration, so we'll need to
  //
  // - take the specifiers from both the main and user configs and make a vector out of them
  // - for each configuration and user config, we'll need to create message lists and figure out
  //   what to do with allow/deny/index.

  let mut protocols: HashMap<String, ProtocolDeviceConfiguration> = HashMap::new();

  // Iterate through all of the protocols in the main config first and build up a map of protocol
  // name to ProtocolDeviceConfiguration structs.
  for (protocol_name, protocol_def) in main_config.protocols.unwrap_or_default() {
    protocols.insert(protocol_name, protocol_def.into());
  }

  let mut external_config = ExternalDeviceConfiguration::default();
  external_config.protocol_configurations = protocols;

  // Then load the user config
  if let Some(user_config) = user_config_str {
    let config = load_protocol_config_from_json(&user_config, skip_version_check)?;
    if let Some(user_configs) = config.user_configs {
      add_user_configs_to_protocol(&mut external_config, user_configs);
    }
  }

  Ok(external_config)
}

pub fn create_test_dcm(allow_raw_messages: bool) -> DeviceConfigurationManager {
  let devices = load_protocol_configs_from_json(None, None, false)
    .expect("If this fails, the whole library goes with it.");
  let dcm = DeviceConfigurationManager::new(allow_raw_messages);
  for (name, def) in devices.protocol_configurations {
    dcm
      .add_protocol_device_configuration(&name, &def)
      .expect("If this fails, the whole library goes with it.");
  }
  dcm
}

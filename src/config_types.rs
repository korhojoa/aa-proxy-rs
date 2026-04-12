use crate::mitm;
use crate::mitm::protos::DisplayType;
use crate::mitm::protos::EvConnectorType;
use bluer::Address;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone)]
pub struct BluetoothAddressList(pub Option<Vec<Address>>);

impl BluetoothAddressList {
    fn to_string_internal(&self) -> String {
        match &self.0 {
            Some(addresses) => addresses
                .iter()
                .map(|addr| addr.to_string())
                .collect::<Vec<_>>()
                .join(","),
            None => "".to_string(),
        }
    }
}

impl<'de> Deserialize<'de> for BluetoothAddressList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(BluetoothAddressList(None));
        }

        let addresses: Result<Vec<Address>, _> = s
            .split(',')
            .map(|addr_str| addr_str.trim().parse::<Address>())
            .collect();

        match addresses {
            Ok(addrs) => {
                let wildcard_present = addrs.iter().any(|addr| addr == &Address::any());

                if wildcard_present && addrs.len() > 1 {
                    return Err(de::Error::custom(
                        "'connect' - Wildcard address '00:00:00:00:00:00' cannot be combined with other addresses"
                    ));
                }
                Ok(BluetoothAddressList(Some(addrs)))
            }
            Err(e) => Err(de::Error::custom(format!(
                "'connect' - Failed to parse addresses: {}",
                e
            ))),
        }
    }
}

impl Serialize for BluetoothAddressList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let addresses_str = self.to_string_internal();
        serializer.serialize_str(&addresses_str)
    }
}

impl fmt::Display for BluetoothAddressList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_internal())
    }
}

impl Default for BluetoothAddressList {
    fn default() -> Self {
        BluetoothAddressList(Some(vec![Address::any()]))
    }
}

fn parse_display_type_token(token: &str) -> Option<DisplayType> {
    match token.trim().to_ascii_lowercase().as_str() {
        "main" | "display_type_main" => Some(DisplayType::DISPLAY_TYPE_MAIN),
        "cluster" | "display_type_cluster" => Some(DisplayType::DISPLAY_TYPE_CLUSTER),
        "aux" | "auxiliary" | "display_type_auxiliary" => Some(DisplayType::DISPLAY_TYPE_AUXILIARY),
        _ => None,
    }
}

impl std::str::FromStr for EvConnectorType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        <mitm::protos::EvConnectorType as protobuf::Enum>::from_str(s.trim())
            .ok_or_else(|| format!("Unknown EV connector type: {}", s))
    }
}

impl std::str::FromStr for DisplayType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_display_type_token(s)
            .or_else(|| <mitm::protos::DisplayType as protobuf::Enum>::from_str(s.trim()))
            .ok_or_else(|| format!("Unknown display type: {}", s))
    }
}

impl fmt::Display for DisplayType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for EvConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvConnectorTypes(pub Option<Vec<EvConnectorType>>);

impl EvConnectorTypes {
    fn to_string_internal(&self) -> String {
        match &self.0 {
            Some(types) => types
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<String>>()
                .join(","),
            None => "".to_string(),
        }
    }
}

impl<'de> Deserialize<'de> for EvConnectorTypes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut types = Vec::new();
        if !s.is_empty() {
            for part in s.split(',') {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    let connector_type = trimmed
                        .parse::<EvConnectorType>()
                        .map_err(de::Error::custom)?;
                    types.push(connector_type);
                }
            }
        }

        if types.is_empty() {
            Ok(EvConnectorTypes(None))
        } else {
            Ok(EvConnectorTypes(Some(types)))
        }
    }
}

impl Serialize for EvConnectorTypes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.to_string_internal();
        serializer.serialize_str(&s)
    }
}

impl fmt::Display for EvConnectorTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.to_string_internal();
        write!(f, "{}", s)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InjectDisplayTypes(pub Option<Vec<DisplayType>>);

impl InjectDisplayTypes {
    fn to_string_internal(&self) -> String {
        match &self.0 {
            Some(types) => types
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<String>>()
                .join(","),
            None => "".to_string(),
        }
    }
}

impl<'de> Deserialize<'de> for InjectDisplayTypes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut types = Vec::new();
        if !s.is_empty() {
            for part in s.split(',') {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    let display_type = trimmed.parse::<DisplayType>().map_err(de::Error::custom)?;
                    if !types.contains(&display_type) {
                        types.push(display_type);
                    }
                }
            }
        }

        if types.is_empty() {
            Ok(InjectDisplayTypes(None))
        } else {
            Ok(InjectDisplayTypes(Some(types)))
        }
    }
}

impl Serialize for InjectDisplayTypes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.to_string_internal();
        serializer.serialize_str(&s)
    }
}

impl fmt::Display for InjectDisplayTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.to_string_internal();
        write!(f, "{}", s)
    }
}

#[derive(
    clap::ValueEnum, Default, Debug, PartialEq, PartialOrd, Clone, Copy, Deserialize, Serialize,
)]
pub enum HexdumpLevel {
    #[default]
    Disabled,
    DecryptedInput,
    RawInput,
    DecryptedOutput,
    RawOutput,
    All,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsbId {
    pub vid: u16,
    pub pid: u16,
}

impl std::str::FromStr for UsbId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err("Expected format VID:PID".to_string());
        }
        let vid = u16::from_str_radix(parts[0], 16).map_err(|e| e.to_string())?;
        let pid = u16::from_str_radix(parts[1], 16).map_err(|e| e.to_string())?;
        Ok(UsbId { vid, pid })
    }
}

impl fmt::Display for UsbId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}:{:x}", self.vid, self.pid)
    }
}

impl<'de> Deserialize<'de> for UsbId {
    fn deserialize<D>(deserializer: D) -> Result<UsbId, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UsbIdVisitor;

        impl<'de> Visitor<'de> for UsbIdVisitor {
            type Value = UsbId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string in the format VID:PID")
            }

            fn visit_str<E>(self, value: &str) -> Result<UsbId, E>
            where
                E: de::Error,
            {
                UsbId::from_str(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(UsbIdVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_display_types_parses_aliases_and_deduplicates() {
        let parsed: InjectDisplayTypes = serde_json::from_str("\"cluster,aux,cluster\"")
            .expect("valid inject display type list");

        assert_eq!(
            parsed,
            InjectDisplayTypes(Some(vec![
                DisplayType::DISPLAY_TYPE_CLUSTER,
                DisplayType::DISPLAY_TYPE_AUXILIARY,
            ]))
        );
    }

    #[test]
    fn inject_display_types_serializes_to_enum_names() {
        let value = InjectDisplayTypes(Some(vec![
            DisplayType::DISPLAY_TYPE_CLUSTER,
            DisplayType::DISPLAY_TYPE_AUXILIARY,
        ]));

        let serialized = serde_json::to_string(&value).expect("serialize inject display types");
        assert_eq!(
            serialized,
            "\"DISPLAY_TYPE_CLUSTER,DISPLAY_TYPE_AUXILIARY\""
        );
    }
}

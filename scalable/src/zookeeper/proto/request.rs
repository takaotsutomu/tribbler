#[repr(i32)]
pub(super) enum OpCode {
    Notification = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetACL = 6,
    SetACL = 7,
    GetChildren = 8,
    Sync = 9,
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Create2 = 15,
    Reconfig = 16,
    CheckWatches = 17,
    RemoveWatches = 18,
    CreateContainer = 19,
    DeleteContainer = 20,
    CreateTTL = 21,
    MultiRead = 22,
    Auth = 100,
    SetWatches = 101,
    Sasl = 102,
    GetEphemerals = 103,
    GetAllChildrenNumber = 104,
    SetWatches2 = 105,
    AddWatch = 106,
    WhoAmI = 107,
    CreateSession = -10,
    CloseSession = -11,
    Connect = -100,
    Error = -1,
}

pub(crate) enum Request {
    Connect {
        protocol_version: i32,
        last_zxid_seen: i64,
        timeout: i32,
        session_id: i64,
        passwd: Vec<u8>,
        read_only: bool,
    },
}

impl Request {
    pub(super) fn serialize_into(&self, buffer: &mut Vec<u8>) -> Result<(), io::Error> {
        match *self {
            Request::Connect { 
                protocol_version,
                last_zxid_seen,
                timeout,
                session_id,
                passwd,
                read_only,
            } => {
                writer.write_i32::<BigEndian>(protocol_version)?;
                writer.write_i64::<BigEndian>(last_zxid_seen)?;
                writer.write_i32::<BigEndian>(timeout)?;
                writer.write_i64::<BigEndian>(session_id)?;
                writer.write_i32::<BigEndian>(passwd.len() as i32)?;
                writer.write_all(passwd)?;
                writer.write_u8(read_only as u8);
                Ok(())
            }
        }
    }

    pub(super) fn opcode(&self) -> super::OpCode {
        match *self {
            Request::Connect => OpCode::Connect,
        }
    }
}
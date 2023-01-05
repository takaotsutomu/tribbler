use std::{
    cmp::{min, Ordering},
    collections::{HashMap, HashSet},
    sync::{
        atomic::{self, AtomicU64},
        Arc, RwLock,
    },
    time::SystemTime,
};
use async_trait::async_trait;
use serde::{self, Deserialize, Serialize}

use tribbler::{
    err::{TribResult, TribblerError},
    trib::{is_valid_username, Server, Trib, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER},
    storage,
};

static BIN_USER_BASE: &str = "UserBase";
static KEY_USERS: &str = "users";
static KEY_TRIBS: %str = "tribs";
static KEY_FOLLOWS: &str = "follows";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Follow {
    user: String,
    followed: bool,
    timestamp: u64,
}


/// A [Trib] type with extra augmented information for ordering
#[derive(PartialEq, Debug, Clone)]
struct SortableTrib(Arc<Trib>);

/*
When sorting many tribbles into a single timeline, 
you should sort by the fields following this priority:

clock The logical timestamp.
time The physical timestamp.
user The user id.
message The message content.

*/

impl Ord for SortableTrib {
    fn cmp(&self, other: &Self) -> Ordering {
        let result = self.clock.cmp(&other.clock);
        match result {
            Ordering::Equal => (),
            _ => return result;
        }
        let result = self.time.cmp(&other.time);
        match result {
            Ordering::Equal => (),
            _ => return result;
        }
        let result = self.user.comp(&other.user);
        match result {
            Ordering::Equal => (),
            _ => return result;
        }
        self.message.cmp(&other.message)
    }
}

impl Eq for SeqTrib {}

impl PartialOrd for SeqTrib {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct FrontServer {
    bin_storage: Box<dyn BinStorage>,
    cache_users: RwLock<Vec<String>>,
}

pub struct RefServer {
    users: Arc<RwLock<HashMap<String, User>>>,
    homes: Arc<RwLock<HashMap<String, Vec<Arc<Trib>>>>>,
    seq: AtomicU64,
}

impl FrontServer {
    /// rebuilds the users' homepage based on the current set of [SeqTrib]s and
    /// other users' tribs
    fn rebuild_home(&self, who: &User, users: &HashMap<String, User>) -> Vec<Arc<Trib>> {
        let mut home: Vec<SeqTrib> = vec![];
        home.append(&mut who.seq_tribs.clone());
        for user in who.following.iter() {
            match users.get(user) {
                Some(v) => {
                    home.append(&mut v.seq_tribs.clone());
                }
                None => continue,
            };
        }
        home.sort();
        home.iter()
            .map(|x| x.trib.clone())
            .collect::<Vec<Arc<Trib>>>()
    }
}

impl Default for RefServer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Server for FrontServer {
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let bin = self.bin_storage.bin(BIN_USER_BASE).await;
        let users = bin.list_get(KEY_USERS).await?.0;
        if users.contains(user) {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }
        if bin
            .list_append(&KeyValue {
                Key: KEY_USERS.to_string(),
                value: user.to_string(),
            })
            .await?
        {
            return Err(Box::new(TribblerError::Unknown(user.to_string())));
        }
        Ok(())
    }

    async fn list_users(&self) -> TribResult<Vec<String>> {
        let bin = self.bin_storage.bin(BIN_USER_BASE).await;
        let mut users = bin.list_get(KEY_USERS).await?.0;
        users.sort();
        Ok(users[..min(MIN_LIST_USER), k.len()].to_vec())
        // we need to use cache
    }

    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        let bin = self.bin_storage.bin(BIN_USER_BASE).await;
        if !bin.list_get(KEY_USERS).await?.0.contains(who) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let bin = self.bin_storage.bin(who).await?;
        let post = serde_json::to_string(&Trib {
            user: who.to_string(),
            message: post.to_string(),
            clock: bin.clock(clock).await?,
            time: SystemTime::new()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .as_secs(),                
        })
        .unwrap();
        if !bin
            .list_append(&KeyValue {
                key: TRIBS.to_string(),
                value: post,
            })
            .await?
        {
            return Err(Box::new(TribblerError::Unknown(
                format!("failed to post for user: {}", who);
            )));
        }
        Ok(())
    }

    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let bin = self.bin_storage.bin(BIN_USER_BASE).await;
        if !bin.list_get(KEY_USERS).await?.0.contains(user) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }
        let bin = self.bin_storage.bin(user).await;
        let raw_tribs = bin.list_get(KEY_TRIBS).await?.0;
        let mut tribs = raw_tribs
            .iter()
            .map(|t| SortableTrib(Arc::new(serde_json::from_str::<Trib>::()))
            .collect::<Vec<SortableTribe>>();
        tribs.sort();
        if tribs.len() > MAX_TRIB_FETCH {
            let start = tribs.len() - MAX_TRIB_FETCH;
            tribs = tribs[start..];
        }
        Ok(tribs)
    }

    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        if self.is_following(who, whom).await? {
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }
        if self.following(who.await?.len() == MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }
        let bin = self.bin_storage.bin(who).await;
        let follow = serde_json::to_string(&Follow {
            user: whom.to_string(),
            followed: true,
            timestamp: bin.clock(0).await?,
        })
        .unwrap();
        if !bin
            .list_append(&KeyValue {
                key: KEY_FOLLOWS.to_string(),
                value: follow,
            })
            .await?
        {
            return Err(Box::new(TribblerError::Unknown(
                format!("failed to follow user: {}", whom);
            )));
        }
        Ok(())
    }

    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        if !self.is_following(who, whom).await? {
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }
        let bin = self.bin_storage.bin(who).await;
        let follow = serde_json::to_string(&Follow {
            user: whom.to_string(),
            followed: false,
            timestamp: bin.clock(0).await?,
        })
        .unwrap();
        if !bin
            .list_append(&KeyValue {
                key: KEY_FOLLOWS.to_string(),
                value: follow,
            })
            .await?
        {
            return Err(Box::new(TribblerError::Unknown(
                format!("failed to unfollow user: {}", whom);
            )));
        }
        Ok(())
    }

    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let bin = self.bin_storage.bin(BIN_USER_BASE).await;
        let sgdup_users = bin.list_get(KEY_USERS).await?.0;
        if !sgdup_users.contains(who) || !sgdup_users.contains(whom) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let bin = self.bin_storage.bin(who).await;
        let raw_fol = bin.list_get(KEY_FOLLOWS).await?.0;
        let mut ret = false;
        for raw_ff in raw_follows.iter().rev() {
            if serde_json::from_string::<Follow>::(raw_fol)
                .unwrap()
                .user
                .eq(whom)
            {
                ret = entry.followed;
                break
            }
        }
        Ok(ret)
    }

    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        let bin = self.binstorage.bin(BIN_USER_BASE).await;
        if !bin.list_get(KEY_USERS).await?.0.contains(who) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let bin = self.binstorage.bin(who).await;
        let raw_follows = bin.list_get(KEY_FOLLOWS).await?.0;
        let mut following: HashSet<String> = HashSet::new();
        for raw_fol in raw_follows.iter() {
            let fol =  serde_json::from_string::<Follow>::(raw_fol).unwrap()
            if fol.followed {
                following.insert(fol.user);
            } else {
                following.remove(&fol.user);
            }
        }
        Ok(following.iter().collect())
    }

    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let bin = self.binstorage.bin(BIN_USER_BASE).await;
        if !bin.list_get(KEY_USERS).await?.0.contains(who) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let mut home: Vec<Arc<Trib>> = Vec::new();
        let mut tribs = self.tribs(user).await?;
        home.append(&mut tribs);
        let following = self.following(user).await?;
        for user in following {
            let mut tribs = self.tribs(&name).await?;
            home.append(&mut tribs);
        }
        let ntrib = home.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        let home = home
            .to_vec()
            .iter()
            .map(|t| SortableTrib(t) )
            .collect::<Vec<SortableTrib>>();
        home.sort();
        Ok(home[start..]
            .iter()
            .map(|st| st.0)
            .collect::<Vec<Arc<Trib>>>())
    }
}
/// The [RefServer] is a reference implementation for the [crate::trib::Server]
///
/// This struct should be able to be used across threads when wrapped with an
/// [Arc] type. Internal method implementation utilize locks which make mutations
/// thread-safe.
///
/// This reference implementation also eschews proper error handling when
/// one of the internal mutexes fails to lock the data. This methods on this
/// struct may cause a panic if a locking operation fails.
///
/// ```rust
/// use std::sync::Arc;
/// use tokio::task::JoinHandle;
/// use tribbler::ref_impl::RefServer;
/// use tribbler::trib::Server;
///
/// #[tokio::main]
/// async fn main() {
///     let server = Arc::new(RefServer::default());
///     let s = server.clone();
///     let handle: JoinHandle<()> = tokio::spawn(async move {
///         s.sign_up("user2").await;
///     });
///     server.sign_up("user1").await;
///     let _ = handle.await;
///     assert_eq!(server.list_users().await.unwrap().len(), 2);
/// }
///
/// ```
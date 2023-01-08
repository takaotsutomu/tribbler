use async_trait::async_trait;
use serde::{self, Deserialize, Serialize};
use std::{
    cmp::{min, Ordering},
    collections::HashSet,
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::Mutex;

use tribbler::{
    err::{TribResult, TribblerError},
    storage::{BinStorage, KeyValue},
    trib::{
        is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
    },
};

static BIN_USER_BASE: &str = "UserBase";
static KEY_USERS: &str = "users";
static KEY_TRIBS: &str = "tribs";
static KEY_FOLLOWS: &str = "follows";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Follow {
    user: String,
    followed: bool,
    timestamp: u64,
}

/// A [Trib] type with extra augmented information for ordering
#[derive(Debug, Clone)]
struct SortableTrib(Arc<Trib>);

impl Ord for SortableTrib {
    fn cmp(&self, other: &Self) -> Ordering {
        let result = self.0.clock.cmp(&other.0.clock);
        match result {
            Ordering::Equal => (),
            _ => {
                return result;
            }
        }
        let result = self.0.time.cmp(&other.0.time);
        match result {
            Ordering::Equal => (),
            _ => {
                return result;
            }
        }
        let result = self.0.user.cmp(&other.0.user);
        match result {
            Ordering::Equal => (),
            _ => {
                return result;
            }
        }
        self.0.message.cmp(&other.0.message)
    }
}

impl Eq for SortableTrib {}

impl PartialOrd for SortableTrib {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortableTrib {
    fn eq(&self, other: &Self) -> bool {
        self.0.clock == other.0.clock
            && self.0.time == other.0.time
            && self.0.user == other.0.user
            && self.0.message == other.0.message
    }
}

pub(crate) struct FrontServer {
    pub(crate) bin_storage: Box<dyn BinStorage>,
    pub(crate) users_cache: Mutex<Vec<String>>,
}

#[async_trait]
impl Server for FrontServer {
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
        let users = bin.list_get(KEY_USERS).await?.0;
        if users.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }
        if !bin
            .list_append(&KeyValue {
                key: KEY_USERS.to_string(),
                value: user.to_string(),
            })
            .await?
        {
            return Err(Box::new(TribblerError::Unknown(user.to_string())));
        }
        Ok(())
    }

    async fn list_users(&self) -> TribResult<Vec<String>> {
        let cache = self.users_cache.lock().await;
        match cache.len().cmp(&MIN_LIST_USER) {
            Ordering::Less => {
                std::mem::drop(cache);
                let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
                let mut users = bin.list_get(KEY_USERS).await?.0;
                users.sort();
                let mut cache = self.users_cache.lock().await;
                *cache = users[..min(MIN_LIST_USER, users.len())].to_vec();
                return Ok(cache.clone());
            }
            _ => Ok(cache.clone()),
        }
    }

    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
        if !bin.list_get(KEY_USERS).await?.0.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let bin = self.bin_storage.bin(who).await?;
        let post = serde_json::to_string(&Trib {
            user: who.to_string(),
            message: post.to_string(),
            clock: bin.clock(clock).await?,
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs(),
        })
        .unwrap();
        if !bin
            .list_append(&KeyValue {
                key: KEY_TRIBS.to_string(),
                value: post,
            })
            .await?
        {
            return Err(Box::new(TribblerError::Unknown(format!(
                "failed to post for user: {}",
                who
            ))));
        }
        Ok(())
    }

    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
        if !bin.list_get(KEY_USERS).await?.0.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }
        let bin = self.bin_storage.bin(user).await?;
        let raw_tribs = bin.list_get(KEY_TRIBS).await?.0;
        let mut stribs = raw_tribs
            .iter()
            .map(|t| SortableTrib(Arc::new(serde_json::from_str::<Trib>(t).unwrap())))
            .collect::<Vec<SortableTrib>>();
        stribs.sort();

        let mut gc = false;
        let ntrib = stribs.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => {
                gc = true;
                ntrib - MAX_TRIB_FETCH
            }
            _ => 0,
        };
        let tribs = stribs[start..]
            .to_vec()
            .iter()
            .map(|st| st.0.clone())
            .collect::<Vec<Arc<Trib>>>();

        // Remove old tribbles if needed, i.e, there are > 100 tribbles
        if gc {
            tribs[..start].to_vec().iter().for_each(|t| {
                match bin.list_remove(&KeyValue {
                    key: KEY_TRIBS.to_string(),
                    value: serde_json::to_string(t).unwrap(),
                }) {
                    _ => {}
                }
            })
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
        if self.following(who).await?.len() == MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }
        let bin = self.bin_storage.bin(who).await?;
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
            return Err(Box::new(TribblerError::Unknown(format!(
                "failed to follow user: {}",
                whom
            ))));
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
        let bin = self.bin_storage.bin(who).await?;
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
            return Err(Box::new(TribblerError::Unknown(format!(
                "failed to unfollow user: {}",
                whom
            ))));
        }
        Ok(())
    }

    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
        let sgdup_users = bin.list_get(KEY_USERS).await?.0;
        if !sgdup_users.contains(&who.to_string()) 
            || !sgdup_users.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let bin = self.bin_storage.bin(who).await?;
        let raw_follows = bin.list_get(KEY_FOLLOWS).await?.0;
        let mut result = false;
        for raw_fol in raw_follows.iter().rev() {
            let fol = serde_json::from_str::<Follow>(raw_fol).unwrap();
            if fol.user.eq(whom) {
                result = fol.followed;
                break;
            }
        }
        Ok(result)
    }

    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
        if !bin.list_get(KEY_USERS).await?.0.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let bin = self.bin_storage.bin(who).await?;
        let raw_follows = bin.list_get(KEY_FOLLOWS).await?.0;
        let mut following: HashSet<String> = HashSet::new();
        for raw_fol in raw_follows.iter() {
            let fol = serde_json::from_str::<Follow>(raw_fol).unwrap();
            if fol.followed {
                following.insert(fol.user);
            } else {
                following.remove(&fol.user);
            }
        }
        Ok(following.into_iter().collect())
    }

    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let bin = self.bin_storage.bin(BIN_USER_BASE).await?;
        if !bin.list_get(KEY_USERS).await?.0.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }
        let mut timeline: Vec<Arc<Trib>> = Vec::new();
        let mut tribs = self.tribs(user).await?;
        // consider whether should do it directly
        timeline.append(&mut tribs);
        let following = self.following(user).await?;
        for fol in following {
            let mut tribs = self.tribs(&fol).await?;
            timeline.append(&mut tribs);
        }
        let mut timeline = timeline
            .to_vec()
            .into_iter()
            .map(|t| SortableTrib(t))
            .collect::<Vec<SortableTrib>>();
        timeline.sort();

        let ntrib = timeline.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        Ok(timeline[start..]
            .to_vec()
            .into_iter()
            .map(|st| st.0.clone())
            .collect::<Vec<Arc<Trib>>>())
    }
}

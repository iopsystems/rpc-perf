use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct GetCompetitionRank {
    pub leaderboard: Arc<String>,
    // TODO: should this be Arc?
    pub ids: Arc<[u32]>,
}

#[derive(Debug, PartialEq)]
pub struct Upsert {
    pub leaderboard: Arc<String>,
    // TODO: Modelled this after sorted set in client.rs
    pub elements: Vec<(u32, f64)>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum LeaderboardClientRequest {
    GetCompetitionRank(GetCompetitionRank),
    Upsert(Upsert),

    Reconnect,
}

use super::*;
use paste::paste;

mod delete;
mod get;
mod hash_delete;
mod hash_get;
mod hash_get_all;
mod hash_increment;
mod hash_set;
mod list_fetch;
mod list_length;
mod list_pop_back;
mod list_pop_front;
mod list_push_back;
mod list_push_front;
mod list_remove;
mod set;
mod set_add;
mod set_members;
mod set_remove;
mod sorted_set_add;
mod sorted_set_increment;
mod sorted_set_range;
mod sorted_set_rank;
mod sorted_set_remove;
mod sorted_set_score;

pub use delete::*;
pub use get::*;
pub use hash_delete::*;
pub use hash_get::*;
pub use hash_get_all::*;
pub use hash_increment::*;
pub use hash_set::*;
pub use list_fetch::*;
pub use list_length::*;
pub use list_remove::*;
pub use list_pop_back::*;
pub use list_pop_front::*;
pub use list_push_back::*;
pub use list_push_front::*;
pub use set::*;
pub use set_add::*;
pub use set_members::*;
pub use set_remove::*;
pub use sorted_set_add::*;
pub use sorted_set_increment::*;
pub use sorted_set_range::*;
pub use sorted_set_rank::*;
pub use sorted_set_remove::*;
pub use sorted_set_score::*;

#[macro_export]
#[rustfmt::skip]
macro_rules! record_result {
    ($result:ident, $command:ident) => {
        paste! {
            match $result {
		        Ok(Ok(_)) => {
		            [<$command _OK>].increment();
		            Ok(())
		        }
		        Ok(Err(e)) => {
		            [<$command _EX>].increment();
		            Err(e.into())
		        }
		        Err(_) => {
		            [<$command _TIMEOUT>].increment();
		            Err(ResponseError::Timeout)
		        }
		    }
        }
    };

    ($result:ident, $command:ident, $ok:ident) => {
        paste! {
            match $result {
		        Ok(Ok(_)) => {
		            [<$ok>].increment();
		            Ok(())
		        }
		        Ok(Err(e)) => {
		            [<$command _EX>].increment();
		            Err(e.into())
		        }
		        Err(_) => {
		            [<$command _TIMEOUT>].increment();
		            Err(ResponseError::Timeout)
		        }
		    }
        }
    };
}

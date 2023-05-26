use super::*;

impl From<&workload::client::Set> for RequestWithValidator {
    fn from(other: &workload::client::Set) -> Self {
        SET.increment();
        RequestWithValidator {
            request: Request::set(
                (*other.key).to_owned().into_boxed_slice(),
                (*other.value).to_owned().into_boxed_slice(),
                0,
                Ttl::none(),
                false,
            ),
            validator: Box::new(validate_response)
        }
    }
}

pub fn validate_response(response: Response) -> std::result::Result<(), ()> {
    match response {
        Response::Stored(_) => {
            SET_STORED.increment();
            Ok(())
        }
        Response::NotStored(_) => {
            SET_NOT_STORED.increment();
            Ok(())
        }
        _ => {
            SET_EX.increment();
            Err(())
        }
    }
}
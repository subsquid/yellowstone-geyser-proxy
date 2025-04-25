use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};


#[derive(Debug, Default, Clone)]
pub struct AuthInterceptor {
    pub x_token: Option<AsciiMetadataValue>,
    pub x_access_token: Option<AsciiMetadataValue>
}


impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", token);
        }
        if let Some(token) = self.x_access_token.clone() {
            request.metadata_mut().insert("x-access-token", token);
        }
        Ok(request)
    }
}
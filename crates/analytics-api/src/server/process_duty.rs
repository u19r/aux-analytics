use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ProcessDuty {
    #[default]
    Combined,
    ApiOnly,
    WorkersOnly,
}

impl ProcessDuty {
    pub(crate) fn resolve_for_endpoint(
        self,
        endpoint: Option<&lambda_web_runtime::LambdaWebEnvironment>,
    ) -> Result<Self, io::Error> {
        match (self, endpoint) {
            (Self::WorkersOnly, Some(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker duty is unavailable in Lambda Web Functions",
            )),
            (_, Some(_)) => Ok(Self::ApiOnly),
            (duty, None) => Ok(duty),
        }
    }

    pub(crate) fn can_run_background_work(self) -> bool {
        !matches!(self, Self::ApiOnly)
    }

    pub(crate) fn bind_address(
        self,
        configured: &str,
        endpoint: Option<&lambda_web_runtime::LambdaWebEnvironment>,
    ) -> Result<SocketAddr, std::net::AddrParseError> {
        if let Some(endpoint) = endpoint {
            return Ok(endpoint.listen_address());
        }
        let mut address: SocketAddr = configured.parse()?;
        if matches!(self, Self::WorkersOnly) {
            address.set_ip(match address.ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
            });
        }
        Ok(address)
    }
}

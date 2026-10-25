pub struct RunPlanOptions<'a> {
    pub family: &'a str,
    pub mode: &'a str,
    pub skip_adapters: bool,
    pub skip_mutations: bool,
}

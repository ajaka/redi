fn main() {
    if let Err(e) = redi::start() {
        eprintln!("{}", e)
    }
}

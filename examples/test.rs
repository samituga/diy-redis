fn main() {
    let frame_bytes = b"+Hello World\r\n";
    
    let expected_content = String::from_utf8(frame_bytes[1..frame_bytes.len()-2].to_vec()).unwrap();
    println!("{expected_content}|")
}
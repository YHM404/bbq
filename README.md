# BBQ: A Block-based Bounded Queue for Exchanging Data and Profiling

This is a concurrent queue that supports multiple producers and multiple consumers. It is implemented based on the paper "BBQ: A Block-based Bounded Queue for Exchanging Data and Profiling" and can be used as follows:

## Usage

Add `bbq` to your `Cargo.toml` dependencies:

```toml
[dependencies]
bbq = "0.1.0"
``` 

## Example:

```rust
use crate::bbq::Bbq;  
use crate::bbq::BlockingQueue;  
  
fn main() {  
	let queue = Bbq::new(100, 100).unwrap();  
  
	// Create four producer threads  
	for i in 0..4 {  
		let q = queue.clone();  
		std::thread::spawn(move || {  
		q.push(i);  
	});

	// Create four consumer threads  
	for _ in 0..4 {  
		let q = queue.clone();  
		std::thread::spawn(move || {  
			println!("{}", q.pop().unwrap());  
		});  
	}
}
```
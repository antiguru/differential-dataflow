extern crate rand;
extern crate timely;
extern crate differential_dataflow;


use timely::dataflow::operators::Probe;
use timely::progress::Antichain;
use differential_dataflow::Config;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{Arrange, TraceAgent};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::trace::{BatchReader, TraceReader};

fn main() {

    let mut args = std::env::args();
    args.next();

    let count: u64 = args.next().unwrap().parse().unwrap();
    let batch: u32 = args.next().unwrap().parse().unwrap();

    let mut timely_config = timely::Config::from_args(args.skip(2)).unwrap();
    let config = Config::default().idle_merge_effort(Some(1000));

    differential_dataflow::configure(&mut timely_config.worker, &config);

    // define a new computational scope, in which to run BFS
    timely::execute(timely_config, move |worker| {

        let index = worker.index();

        // create a degree counting differential dataflow
        let (mut input, probe, mut trace) = worker.dataflow::<u32,_,_>(|scope| {

            // create edge input, count a few ways.
            let (input, edges) = scope.new_collection::<_,i32>();

            let arranged = edges.arrange::<OrdValSpine<u64, _, _, _>>();

            (input, arranged.stream.probe(), arranged.trace)
        });

        let trace_len = |trace: &mut TraceAgent<OrdValSpine<_, _, _, _>>| {
            let mut len = 0;
            println!("--->>>");
            println!("logical compaction: {:?}", trace.get_logical_compaction().iter().cloned().next());
            println!("physical compaction: {:?}", trace.get_physical_compaction().iter().cloned().next());
            let mut f = Antichain::new();
            trace.read_upper(&mut f);
            println!("upper: {:?}", f.iter().next().unwrap());
            trace.map_batches(|batch| {
                println!("batch.layer.keys {}/{}", batch.layer.keys.len(), batch.layer.keys.capacity());
                println!("batch.layer.offs {}/{}", batch.layer.offs.len(), batch.layer.offs.capacity());
                println!("batch.layer.vals.keys {}/{}", batch.layer.vals.keys.len(), batch.layer.vals.keys.capacity());
                println!("batch.layer.vals.offs {}/{}", batch.layer.vals.offs.len(), batch.layer.vals.offs.capacity());
                println!("batch.layer.vals.vals.vals.vals {}/{}", batch.layer.vals.vals.vals.len(), batch.layer.vals.vals.vals.capacity());
                len += batch.len()
            });
            println!("---<<<");
            len
        };

        let enter = |name: &str| {
            println!("{name} done; Press enter...");
            let mut s = String::new();
            let _ = std::io::stdin().read_line(&mut s);
        };

        // Just have worker zero drive input production.
        if index == 0 {
            let mut i = 0;
            let mut next = batch;
            for round in 1 .. {
                probe.with_frontier(|f| {
                    trace.set_logical_compaction(f);
                    trace.set_physical_compaction(f);
                });
                input.advance_to(round);
                input.update((i, i), 1);
                i += 1;

                if round > next {
                    let timer = ::std::time::Instant::now();
                    input.flush();
                    while probe.less_than(input.time()) {
                        worker.step();
                    }
                    println!("round {} finished after {:?}, trace len {}", next, timer.elapsed(), trace_len(&mut trace));
                    next += batch;
                }
                if i > count {
                    break;
                }
            }
            enter("Loading");

            println!("{i}");
            for x in 0 .. i / 2 {
                input.update((x, x), -1);
            }
            input.advance_to(input.time() + 1);
            let timer = ::std::time::Instant::now();
            input.flush();
            probe.with_frontier(|f| {
                trace.set_logical_compaction(f);
                trace.set_physical_compaction(f);
            });
            while probe.less_than(input.time()) {
                worker.step();
            }
            probe.with_frontier(|f| {
                trace.set_logical_compaction(f);
                trace.set_physical_compaction(f);
            });
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            for _ in 0..1000000 {
                worker.step();
            }
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            enter("Retracting");

            input.advance_to(input.time() + 1);
            let timer = ::std::time::Instant::now();
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            probe.with_frontier(|f| {
                trace.set_logical_compaction(f);
                trace.set_physical_compaction(f);
            });
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            for _ in 0..1000000 {
                worker.step();
            }
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            enter("Advancing time");

            input.advance_to(input.time() + 1);
            let timer = ::std::time::Instant::now();
            input.flush();
            probe.with_frontier(|f| {
                trace.set_logical_compaction(f);
                trace.set_physical_compaction(f);
            });
            while probe.less_than(input.time()) {
                worker.step();
            }
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            for _ in 0..1000000 {
                worker.step();
            }
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            enter("Advancing time II");

            input.advance_to(input.time() + 1);
            input.update((1, 1), 1);
            let timer = ::std::time::Instant::now();
            input.flush();
            probe.with_frontier(|f| {
                trace.set_logical_compaction(f);
                trace.set_physical_compaction(f);
            });
            while probe.less_than(input.time()) {
                worker.step();
            }
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));
            for _ in 0..1000000 {
                worker.step();
            }
            println!("round {} finished after {:?}, trace len {}", input.time(), timer.elapsed(), trace_len(&mut trace));

            println!("Inserting single record done");
        }
    }).unwrap();
}

//! Wordcount based on `columnar`.
use differential_containers::columnation::{ColKeyBuilder, ColKeySpine};
use differential_containers::columnar::{Col2KeyBatcher, Column, ColumnBuilder};
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use timely::container::{Container, CapacityContainerBuilder};
use timely::dataflow::InputHandleCore;
use timely::dataflow::ProbeHandle;
use timely::dataflow::channels::pact::ExchangeCore;

fn main() {

    type WordCount = ((String, ()), u64, i64);
    type Container = Column<WordCount>;

    let _config = timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(3),
        worker: timely::WorkerConfig::default(),
    };

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1.clone();

    // initializes and runs a timely dataflow.
    // timely::execute(_config, move |worker| {
    timely::execute_from_args(std::env::args(), move |worker| {

        let mut data_input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut keys_input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<u64, _, _>(|scope| {

            let data = data_input.to_stream(scope);
            let keys = keys_input.to_stream(scope);

            let data_pact = ExchangeCore::<ColumnBuilder<((String,()),u64,i64)>,_>::new_core(|x: &((&str,()),&u64,&i64)| (x.0).0.as_bytes().iter().map(|x| *x as u64).sum::<u64>() as u64);
            let keys_pact = ExchangeCore::<ColumnBuilder<((String,()),u64,i64)>,_>::new_core(|x: &((&str,()),&u64,&i64)| (x.0).0.as_bytes().iter().map(|x| *x as u64).sum::<u64>() as u64);

            let data = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>(&data, data_pact, "Data");
            let keys = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>(&keys, keys_pact, "Keys");

            keys.join_core(&data, |_k, &(), &()| Option::<()>::None)
                .probe_with(&mut probe);

        });

        // Resources for placing input data in containers.
        use std::fmt::Write;
        let mut buffer = String::default();
        let mut container = Container::default();

        // Load up data in batches.
        let mut counter = 0;
        while counter < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (counter + i) % keys;
                write!(buffer, "{:?}", val).unwrap();
                container.push(((&buffer, ()), time, 1));
                buffer.clear();
                i += worker.peers();
            }
            data_input.send_batch(&mut container);
            container.clear();
            counter += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        let mut queries = 0;

        while queries < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (queries + i) % keys;
                write!(buffer, "{:?}", val).unwrap();
                container.push(((&buffer, ()), time, 1));
                buffer.clear();
                i += worker.peers();
            }
            data_input.send_batch(&mut container);
            container.clear();
            queries += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }

        println!("{:?}\tqueries complete", timer1.elapsed());


    })
    .unwrap();

    println!("{:?}\tshut down", timer2.elapsed());
}



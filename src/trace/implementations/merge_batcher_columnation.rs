//! A general purpose `Batcher` implementation based on radix sort for TimelyStack.

use timely::Container;
use timely::communication::message::RefOrMut;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::progress::frontier::Antichain;

use ::difference::Semigroup;

use lattice::Lattice;
use trace::{Batch, Batcher, Builder};

impl<D: Data+Columnation, T: Data+Columnation, R: Semigroup+Columnation> MergeContainer for TimelyStack<(D, T, R)> {
    type MergeBatcher = MergeSorterColumnation<D, T, R>;
}

/// Creates batches from unordered tuples.
pub struct TimelyStackMergeBatcher<B: Batch>
    where
        B::Key: Data+Columnation,
        B::Val: Data+Columnation,
        B::Time: Lattice+timely::progress::Timestamp+Ord+Clone+Columnation,
        B::R: Semigroup+Columnation,
{
    sorter: MergeSorterColumnation<(B::Key, B::Val), B::Time, B::R>,
    lower: Antichain<B::Time>,
    frontier: Antichain<B::Time>,
    phantom: ::std::marker::PhantomData<B>,
}

impl<B: Batch> Batcher<B> for TimelyStackMergeBatcher<B>
where
    B::Key: Data+Columnation,
    B::Val: Data+Columnation,
    B::Time: Lattice+timely::progress::Timestamp+Ord+Clone+Columnation,
    B::R: Semigroup+Columnation,
{
    type Input = TimelyStack<((B::Key, B::Val), B::Time, B::R)>;
    fn new() -> Self {
        TimelyStackMergeBatcher {
            sorter: MergeSorterColumnation::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(<B::Time as timely::progress::Timestamp>::minimum()),
            phantom: ::std::marker::PhantomData,
        }
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: RefOrMut<Self::Input>) {
        // `batch` is either a shared reference or an owned allocations.
        match batch {
            RefOrMut::Ref(reference) => {
                // This is a moment at which we could capture the allocations backing
                // `batch` into a different form of region, rather than just  cloning.
                let mut owned: Self::Input = self.sorter.empty();
                owned.clone_from(reference);
                self.sorter.push(&mut owned);
            },
            RefOrMut::Mut(reference) => {
                self.sorter.push(reference);
            }
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline(never)]
    fn seal(&mut self, upper: Antichain<B::Time>) -> B {

        let mut builder = B::Builder::new();

        let mut merged = Default::default();
        self.sorter.finish_into(&mut merged);

        let mut kept = Vec::new();
        let mut keep = TimelyStack::default();

        self.frontier.clear();

        // TODO: Re-use buffer, rather than dropping.
        for mut buffer in merged.drain(..) {
            for datum @ ((key, val), time, diff) in &buffer[..] {
                if upper.less_equal(time) {
                    self.frontier.insert(time.clone());
                    if keep.len() == keep.capacity() {
                        if keep.len() > 0 {
                            kept.push(keep);
                            keep = self.sorter.empty();
                        }
                    }
                    keep.copy(datum);
                }
                else {
                    builder.push((key.clone(), val.clone(), time.clone(), diff.clone()));
                }
            }
            buffer.clear();
            // Recycling buffer.
            self.sorter.push(&mut buffer);
        }

        // Finish the kept data.
        if keep.len() > 0 {
            kept.push(keep);
        }
        if kept.len() > 0 {
            self.sorter.push_list(kept);
        }

        // Drain buffers (fast reclaimation).
        // TODO : This isn't obviously the best policy, but "safe" wrt footprint.
        //        In particular, if we are reading serialized input data, we may
        //        prefer to keep these buffers around to re-fill, if possible.
        let mut buffer = Default::default();
        self.sorter.push(&mut buffer);
        // We recycle buffers with allocations (capacity, and not zero-sized).
        while buffer.capacity() > 0 && std::mem::size_of::<((B::Key,B::Val),B::Time,B::R)>() > 0 {
            buffer = Default::default();
            self.sorter.push(&mut buffer);
        }

        let seal = builder.done(self.lower.clone(), upper.clone(), Antichain::from_elem(<B::Time as timely::progress::Timestamp>::minimum()));
        self.lower = upper;
        seal
    }

    // the frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<B::Time> {
        self.frontier.borrow()
    }
}


use consolidation::Consolidation;
use Data;
use trace::implementations::merge_batcher::VecQueue;
use trace::layers::{ContainerMergeBatcher, MergeContainer};

pub struct TimelyStackQueue<T: Columnation> {
    list: TimelyStack<T>,
    head: usize,
}

impl<T: Columnation + 'static> TimelyStackQueue<T> {
    #[inline]
    pub fn new() -> Self { TimelyStackQueue::from(Default::default()) }
    #[inline]
    pub fn pop(&mut self) -> &T {
        self.head += 1;
        &self.list[self.head - 1]
    }
    #[inline]
    pub fn peek(&self) -> &T {
        &self.list[self.head]
    }
    #[inline]
    pub fn from(list: TimelyStack<T>) -> Self {
        TimelyStackQueue {
            list,
            head: 0,
        }
    }
    #[inline]
    pub fn done(mut self) -> TimelyStack<T> {
        self.list.clear();
        self.list
    }
    #[inline]
    pub fn len(&self) -> usize { self.list.len() - self.head }
    #[inline]
    pub fn is_empty(&self) -> bool { self.head == self.list.len() }
}

pub struct MergeSorterColumnation<D: Ord+Columnation, T: Ord+Columnation, R: Semigroup+Columnation> {
    queue: Vec<Vec<TimelyStack<(D, T, R)>>>,    // each power-of-two length list of allocations.
    stash: Vec<TimelyStack<(D, T, R)>>,
}

impl<D: Data+Columnation, T: Data+Columnation, R: Semigroup+Columnation> MergeSorterColumnation<D, T, R> {

    const BUFFER_SIZE_BYTES: usize = 1 << 13;

    fn buffer_size() -> usize {
        let size = ::std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    #[inline]
    pub fn new() -> Self { MergeSorterColumnation { queue: Vec::new(), stash: Vec::new() } }

    #[inline]
    pub fn empty(&mut self) -> TimelyStack<(D, T, R)> {
        self.stash.pop().unwrap_or_else(|| TimelyStack::with_capacity(Self::buffer_size()))
    }

    #[inline(never)]
    pub fn _sort(&mut self, list: &mut Vec<TimelyStack<(D, T, R)>>) {
        for mut batch in list.drain(..) {
            self.push(&mut batch);
        }
        self.finish_into(list);
    }

    #[inline]
    pub fn push(&mut self, batch: &mut TimelyStack<(D, T, R)>) {
        // TODO: Reason about possible unbounded stash growth. How to / should we return them?
        // TODO: Reason about mis-sized vectors, from deserialized data; should probably drop.
        let mut batch = if self.stash.len() > 2 {
            ::std::mem::replace(batch, self.stash.pop().unwrap())
        }
        else {
            ::std::mem::replace(batch, Default::default())
        };

        if batch.len() > 0 {
            batch.consolidate();
            self.queue.push(vec![batch]);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want
    // to break it down to be so.
    pub fn push_list(&mut self, list: Vec<TimelyStack<(D, T, R)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }
        self.queue.push(list);
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<TimelyStack<(D, T, R)>>) {
        while self.queue.len() > 1 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }

        if let Some(mut last) = self.queue.pop() {
            ::std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<TimelyStack<(D, T, R)>>, list2: Vec<TimelyStack<(D, T, R)>>) -> Vec<TimelyStack<(D, T, R)>> {

        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = VecQueue::from(list1);
        let mut list2 = VecQueue::from(list2);

        let mut head1 = if !list1.is_empty() { TimelyStackQueue::from(list1.pop()) } else { TimelyStackQueue::new() };
        let mut head2 = if !list2.is_empty() { TimelyStackQueue::from(list2.pop()) } else { TimelyStackQueue::new() };

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && head1.len() > 0 && head2.len() > 0 {

                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                match cmp {
                    Ordering::Less    => { result.copy(head1.pop()); }
                    Ordering::Greater => { result.copy(head2.pop()); }
                    Ordering::Equal   => {
                        let (data1, time1, diff1) = head1.pop();
                        let (_data2, _time2, diff2) = head2.pop();
                        let mut diff1 = diff1.clone();
                        diff1.plus_equals(&diff2);
                        if !diff1.is_zero() {
                            result.copy(&(data1.clone(), time1.clone(), diff1));
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.empty();
            }

            if head1.is_empty() {
                let done1 = head1.done();
                if done1.capacity() == Self::buffer_size() { self.stash.push(done1); }
                head1 = if !list1.is_empty() { TimelyStackQueue::from(list1.pop()) } else { TimelyStackQueue::new() };
            }
            if head2.is_empty() {
                let done2 = head2.done();
                if done2.capacity() == Self::buffer_size() { self.stash.push(done2); }
                head2 = if !list2.is_empty() { TimelyStackQueue::from(list2.pop()) } else { TimelyStackQueue::new() };
            }
        }

        if result.len() > 0 { output.push(result); }
        else if result.capacity() > 0 { self.stash.push(result); }

        if !head1.is_empty() {
            let mut result = self.empty();
            for _ in 0 .. head1.len() { result.copy(head1.pop()); }
            output.push(result);
        }
        while !list1.is_empty() {
            output.push(list1.pop());
        }

        if !head2.is_empty() {
            let mut result = self.empty();
            for _ in 0 .. head2.len() { result.copy(head2.pop()); }
            output.push(result);
        }
        while !list2.is_empty() {
            output.push(list2.pop());
        }

        output
    }
}

impl<D: Data+Columnation, T: Data+Columnation, R: Semigroup+Columnation> ContainerMergeBatcher<TimelyStack<(D, T, R)>> for MergeSorterColumnation<D, T, R> {
    fn empty(&mut self) -> TimelyStack<(D, T, R)> {
        MergeSorterColumnation::empty(self)
    }

    fn push(&mut self, batch: &mut TimelyStack<(D, T, R)>) {
        MergeSorterColumnation::push(self, batch)
    }

    fn finish_into(&mut self, target: &mut Vec<TimelyStack<(D, T, R)>>) {
        MergeSorterColumnation::finish_into(self, target)
    }
}

impl<D: Data+Columnation, T: Data+Columnation, R: Semigroup+Columnation> Default for MergeSorterColumnation<D, T, R> {
    fn default() -> Self {
        Self::new()
    }
}

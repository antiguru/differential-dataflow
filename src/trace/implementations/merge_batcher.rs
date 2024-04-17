//! A general purpose `Batcher` implementation based on radix sort.

use timely::communication::message::RefOrMut;
use timely::container::PushInto;
use timely::logging::WorkerIdentifier;
use timely::logging_core::Logger;
use timely::progress::{frontier::Antichain, Timestamp};
use crate::consolidation::consolidate_updates;

use crate::difference::Semigroup;
use crate::logging::{BatcherEvent, DifferentialEvent};
use crate::trace::{Batcher, Builder};

struct VectorUpdate<D, T, R>(Vec<(D,T,R)>);
impl<D,T,R> Default for VectorUpdate<D,T,R> {
    fn default() -> Self {
        Self(Vec::default())
    }
}
impl<D,T,R> VectorUpdate<D,T,R> {
    fn capacity(&self) -> usize {
        self.0.capacity()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
// impl<D,T,R> PushInto<VectorUpdate<D,T,R>> for (&D, &T, &R) {
//     fn push_into(self, target: &mut VectorUpdate<D, T, R>) {
//         todo!()
//     }
// }
impl<K,V,T,D> PushInto<VectorUpdate<(K,V),T,D>> for ((K,V),T,D) {
    fn push_into(self, target: &mut VectorUpdate<(K, V), T, D>) {
        target.0.push(self)
    }
}
impl<K: Clone,V: Clone,T: Clone,D: Clone> PushInto<VectorUpdate<(K,V),T,D>> for &((K,V),T,D) {
    fn push_into(self, target: &mut VectorUpdate<(K, V), T, D>) {
        target.0.push(self.clone())
    }
}
impl<'a, D: Ord,T: Ord,R:Semigroup + Clone+InternalToOwned> PushInto<VectorUpdate<D,T,R>> for (
    <<VectorUpdate<D,T,R> as InternalContainer>::Item<'a> as DataUpdate>::Data<'a>,
    <<VectorUpdate<D,T,R> as InternalContainer>::Item<'a> as DataUpdate>::Time<'a>,
    <<VectorUpdate<D,T,R> as InternalContainer>::Item<'a> as DataUpdate>::Diff<'a>
) {
    fn push_into(self, target: &mut VectorUpdate<D, T, R>) {
        todo!()
    }
}

struct BBOWrapper<T>(T);

impl<'a, D: Ord,T: Ord,R:Semigroup + Clone+InternalToOwned> PushInto<VectorUpdate<D,T,R>> for BBOWrapper<(
    <<VectorUpdate<D,T,R> as InternalContainer>::Item<'a> as DataUpdate>::Data<'a>,
    <<VectorUpdate<D,T,R> as InternalContainer>::Item<'a> as DataUpdate>::Time<'a>,
    <<<VectorUpdate<D,T,D> as InternalContainer>::Item<'a> as DataUpdate>::Diff<'a> as InternalToOwned>::Owned
    )> {
    fn push_into(self, target: &mut VectorUpdate<D, T, R>) {
        todo!()
    }
}

trait InternalContainer: Default {
    type Item<'a> where Self: 'a;
    type DrainItem<'a> where Self: 'a;
    type Iter<'a>: Iterator<Item=Self::Item<'a>> where Self: 'a;
    type DrainIter<'a>: Iterator<Item=Self::DrainItem<'a>> where Self: 'a;
    fn with_capacity(capacity: usize) -> Self;
    fn preferred_capacity() -> usize;
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> Self::Iter<'_>;
    fn drain(&mut self) -> Self::DrainIter<'_>;
    fn index(&self, index: usize) -> Self::Item<'_>;
}

trait InternalToOwned {
    type Owned: Semigroup + 'static;
    fn to_owned(self) -> Self::Owned;
}

impl<D,T,R> InternalContainer for VectorUpdate<D,T,R> {
    type Item<'a> = &'a (D,T,R) where Self: 'a;
    type DrainItem<'a> = (D,T,R) where Self: 'a;
    type Iter<'a> = std::slice::Iter<'a, (D,T,R)> where Self: 'a;
    type DrainIter<'a> = std::vec::Drain<'a, (D,T,R)> where Self: 'a;
    fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
    fn preferred_capacity() -> usize {
        const BUFFER_SIZE_BYTES: usize = 1 << 13;

        let size = ::std::mem::size_of::<D>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn iter(&self) -> Self::Iter<'_> {
        self.0.iter()
    }
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.0.drain(..)
    }
    fn capacity(&self) -> usize {
        self.0.capacity()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn index(&self, index: usize) -> Self::Item<'_> {
        &self.0[index]
    }
}

trait DataUpdate {
    type Data<'a>: Ord where Self: 'a;
    type Time<'a>: Ord where Self: 'a;
    type Diff<'a>: InternalToOwned where Self: 'a;

    fn data(&self) -> Self::Data<'_>;
    fn time(&self) -> Self::Time<'_>;
    fn diff(&self) -> Self::Diff<'_>;
}

impl<D: Ord, T: Ord, R: Semigroup + Clone + InternalToOwned> DataUpdate for &(D, T, R) {
    type Data<'a> = &'a D where Self: 'a;
    type Time<'a> = &'a T where Self: 'a;
    type Diff<'a> = &'a R where Self: 'a;

    fn data(&self) -> Self::Data<'_> {
        &self.0
    }

    fn time(&self) -> Self::Time<'_> {
        &self.1
    }

    fn diff(&self) -> Self::Diff<'_> {
        &self.2
    }
}

/// Creates batches from unordered tuples.
pub struct MergeBatcher<K, V, T, D> {
    sorter: MergeSorter<VectorUpdate<(K, V), T, D>>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
}

impl<T: Clone + Semigroup> InternalToOwned for &T {
    type Owned = T;

    fn to_owned(self) -> Self::Owned {
        self.clone()
    }
}

impl<K, V, T, D> Batcher for MergeBatcher<K, V, T, D>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Timestamp + 'static,
    D: InternalToOwned + Semigroup + 'static,
    for<'a> <VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a>: DataUpdate,
    for<'a> <VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a>: PushInto<VectorUpdate<(K,V),T,D>>,
    for<'a> <<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Diff<'a>: InternalToOwned,
    for<'a> <<<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Diff<'a> as InternalToOwned>::Owned: Semigroup,
    for<'a> (
        <<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Data<'a>,
        <<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Time<'a>,
        <<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Diff<'a>
    ): PushInto<VectorUpdate<(K,V),T,D>>,
    for<'a> BBOWrapper<(
        <<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Data<'a>,
        <<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Time<'a>,
        <<<VectorUpdate<(K,V),T,D> as InternalContainer>::Item<'a> as DataUpdate>::Diff<'a> as InternalToOwned>::Owned
    )>: PushInto<VectorUpdate<(K,V),T,D>>,
{
    type Input = Vec<((K,V),T,D)>;
    type Output = ((K,V),T,D);
    type OutputBatch = Vec<((K,V),T,D)>;
    type Time = T;

    fn new(logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>, operator_id: usize) -> Self {
        MergeBatcher {
            sorter: <MergeSorter<VectorUpdate<(K,V),T,D>>>::new(logger, operator_id),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(T::minimum()),
        }
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: RefOrMut<Self::Input>) {
        // `batch` is either a shared reference or an owned allocations.
        let mut vec = match batch {
            RefOrMut::Ref(reference) => {
                // This is a moment at which we could capture the allocations backing
                // `batch` into a different form of region, rather than just  cloning.
                let mut owned: Vec<_> = Vec::default();
                owned.clone_from(reference);
                owned
            },
            RefOrMut::Mut(reference) => {
                std::mem::take(reference)
            }
        };
        consolidate_updates(&mut vec);

        self.sorter.push(&mut VectorUpdate(vec));
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline(never)]
    fn seal<B: Builder<InputBatch=Self::OutputBatch, Input=Self::Output, Time=Self::Time>>(&mut self, upper: Antichain<T>) -> B::Output {

        let mut merged = Vec::new();
        self.sorter.finish_into(&mut merged);

        // Determine the number of distinct keys, values, and updates,
        // and form a builder pre-sized for these numbers.
        let mut builder = {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            let mut prev_keyval = None;
            for buffer in merged.iter() {
                for ((key, val), time, _) in buffer.iter() {
                    if !upper.less_equal(time) {
                        if let Some((p_key, p_val)) = prev_keyval {
                            if p_key != key {
                                keys += 1;
                                vals += 1;
                            }
                            else if p_val != val {
                                vals += 1;
                            }
                            upds += 1;
                        } else {
                            keys += 1;
                            vals += 1;
                            upds += 1;
                        }
                        prev_keyval = Some((key, val));
                    }
                }
            }
            B::with_capacity(keys, vals, upds)
        };

        let mut kept = Vec::new();
        let mut keep = VectorUpdate::default();

        self.frontier.clear();

        // TODO: Re-use buffer, rather than dropping.
        for mut buffer in merged.drain(..) {
            for ((key, val), time, diff) in buffer.drain() {
                if upper.less_equal(&time) {
                    self.frontier.insert(time.clone());
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.sorter.empty();
                    }
                    ((key, val), time, diff).push_into(&mut keep);
                }
                else {
                    builder.push(((key, val), time, diff));
                }
            }
            // Recycling buffer.
            self.sorter.push(&mut buffer);
        }

        // Finish the kept data.
        if !keep.is_empty() {
            kept.push(keep);
        }
        if !kept.is_empty() {
            self.sorter.push_list(kept);
        }

        // Drain buffers (fast reclaimation).
        // TODO : This isn't obviously the best policy, but "safe" wrt footprint.
        //        In particular, if we are reading serialized input data, we may
        //        prefer to keep these buffers around to re-fill, if possible.
        let mut buffer = VectorUpdate::default();
        self.sorter.push(&mut buffer);
        // We recycle buffers with allocations (capacity, and not zero-sized).
        while buffer.capacity() > 0 && std::mem::size_of::<((K,V),T,D)>() > 0 {
            buffer = VectorUpdate::default();
            self.sorter.push(&mut buffer);
        }

        let seal = builder.done(self.lower.clone(), upper.clone(), Antichain::from_elem(T::minimum()));
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<T> {
        self.frontier.borrow()
    }
}

struct MergeSorter<C: InternalContainer> {
    /// each power-of-two length list of allocations. Do not push/pop directly but use the corresponding functions.
    queue: Vec<Vec<C>>,
    stash: Vec<C>,
    logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
    operator_id: usize,
}

impl<C: InternalContainer> MergeSorter<C>
where
    for<'a> C::Item<'a>: DataUpdate,
    for<'a> C::Item<'a>: PushInto<C>,
    for<'a> <C::Item<'a> as DataUpdate>::Diff<'a>: InternalToOwned,
    for<'a> <<C::Item<'a> as DataUpdate>::Diff<'a> as InternalToOwned>::Owned: Semigroup,
    for<'a> (<C::Item<'a> as DataUpdate>::Data<'a>, <C::Item<'a> as DataUpdate>::Time<'a>, <C::Item<'a> as DataUpdate>::Diff<'a>): PushInto<C>,
    for<'a> BBOWrapper<(<C::Item<'a> as DataUpdate>::Data<'a>, <C::Item<'a> as DataUpdate>::Time<'a>, <<C::Item<'a> as DataUpdate>::Diff<'a> as InternalToOwned>::Owned)>: PushInto<C>,
{

    fn buffer_size() -> usize {
        C::preferred_capacity()
    }

    #[inline]
    fn new(logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            queue: Vec::new(),
            stash: Vec::new(),
        }
    }

    #[inline]
    pub fn empty(&mut self) -> C {
        self.stash.pop().unwrap_or_else(|| C::with_capacity(Self::buffer_size()))
    }

    #[inline]
    pub fn push(&mut self, batch: &mut C) {
        // TODO: Reason about possible unbounded stash growth. How to / should we return them?
        // TODO: Reason about mis-sized vectors, from deserialized data; should probably drop.
        let mut batch = if self.stash.len() > 2 {
            ::std::mem::replace(batch, self.stash.pop().unwrap())
        }
        else {
            ::std::mem::take(batch)
        };

        if !batch.is_empty() {
            // crate::consolidation::consolidate_updates(&mut batch);
            self.account([batch.len()], 1);
            self.queue_push(vec![batch]);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue_pop().unwrap();
                let list2 = self.queue_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue_push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want
    // to break it down to be so.
    pub fn push_list(&mut self, list: Vec<C>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }
        self.queue_push(list);
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<C>) {
        while self.queue.len() > 1 {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }

        if let Some(mut last) = self.queue_pop() {
            ::std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<C>, list2: Vec<C>) -> Vec<C> {
        self.account(list1.iter().chain(list2.iter()).map(InternalContainer::len), -1);

        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = StackQueue::from(list1.next().unwrap_or_default());
        let mut head2 = StackQueue::from(list2.next().unwrap_or_default());

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {

                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (x.data(), x.time()).cmp(&(y.data(), y.time()))
                };
                match cmp {
                    Ordering::Less    => head1.pop().push_into(&mut result),
                    Ordering::Greater => head2.pop().push_into(&mut result),
                    Ordering::Equal   => {
                        let item1 = head1.pop();
                        let item2 = head2.pop();
                        let mut diff1 = item1.diff().to_owned();
                        // TODO: Remove `to_owned`!
                        diff1.plus_equals(&item2.diff().to_owned());
                        if !diff1.is_zero() {
                            (item1.diff(), item1.time(), diff1).push_into(&mut result);
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
                head1 = StackQueue::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                let done2 = head2.done();
                if done2.capacity() == Self::buffer_size() { self.stash.push(done2); }
                head2 = StackQueue::from(list2.next().unwrap_or_default());
            }
        }

        if !result.is_empty() { output.push(result); }
        else if result.capacity() > 0 { self.stash.push(result); }

        if !head1.is_empty() {
            let mut result = self.empty();
            for item1 in head1.iter() { item1.push_into(&mut result); }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty();
            for item2 in head2.iter() { item2.push_into(&mut result); }
            output.push(result);
        }
        output.extend(list2);

        output
    }
}

impl<C: InternalContainer> MergeSorter<C> {
    /// Pop a batch from `self.queue` and account size changes.
    #[inline]
    fn queue_pop(&mut self) -> Option<Vec<C>> {
        let batch = self.queue.pop();
        self.account(batch.iter().flatten().map(InternalContainer::len), -1);
        batch
    }

    /// Push a batch to `self.queue` and account size changes.
    #[inline]
    fn queue_push(&mut self, batch: Vec<C>) {
        self.account(batch.iter().map(InternalContainer::len), 1);
        self.queue.push(batch);
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the [`TimelyStack`]s passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    fn account<I: IntoIterator<Item=usize>>(&self, items: I, diff: isize) {
        if let Some(logger) = &self.logger {
            let mut records= 0isize;
            for len in items {
                records = records.saturating_add_unsigned(len);
            }
            logger.log(BatcherEvent {
                operator: self.operator_id,
                records_diff: records * diff,
                size_diff: 0,
                capacity_diff: 0,
                allocations_diff: 0,
            })
        }
    }
}

impl<C: InternalContainer> Drop for MergeSorter<C> {
    fn drop(&mut self) {
        while self.queue_pop().is_some() { }
    }
}

struct StackQueue<C> {
    list: C,
    head: usize,
}

impl<C: Default + InternalContainer> Default for StackQueue<C> {
    fn default() -> Self {
        Self::from(C::default())
    }
}

impl<C: InternalContainer> StackQueue<C> {

    fn pop(&mut self) -> C::Item<'_> {
        self.head += 1;
        self.list.index(self.head - 1)
    }

    fn peek(&self) -> C::Item<'_> {
        self.list.index(self.head)
    }

    fn from(list: C) -> Self {
        StackQueue {
            list,
            head: 0,
        }
    }

    fn done(self) -> C {
        self.list
    }

    fn is_empty(&self) -> bool { self.head == self.list.len() }

    /// Return an iterator over the remaining elements.
    fn iter(&self) -> impl Iterator<Item=C::Item<'_>> {
        InternalContainer::iter(&self.list).take(self.head)
    }
}

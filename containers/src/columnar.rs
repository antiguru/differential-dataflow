use std::rc::Rc;
pub use container::Column;
mod container {

    use columnar::Columnar;
    use columnar::Container as _;

    use timely::bytes::arc::Bytes;

    /// A container based on a columnar store, encoded in aligned bytes.
    pub enum Column<C: Columnar> {
        /// The typed variant of the container.
        Typed(C::Container),
        /// The binary variant of the container.
        Bytes(Bytes),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Reasons could include misalignment, cloning of data, or wanting
        /// to release the `Bytes` as a scarce resource.
        Align(Box<[u64]>),
    }

    impl<C: Columnar> Default for Column<C> {
        fn default() -> Self { Self::Typed(Default::default()) }
    }

    impl<C: Columnar> Clone for Column<C> where C::Container: Clone {
        fn clone(&self) -> Self {
            match self {
                Column::Typed(t) => Column::Typed(t.clone()),
                Column::Bytes(b) => {
                    assert!(b.len() % 8 == 0);
                    let mut alloc: Vec<u64> = vec![0; b.len() / 8];
                    bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&b[..]);
                    Self::Align(alloc.into())
                },
                Column::Align(a) => Column::Align(a.clone()),
            }
        }
    }

    use columnar::{Clear, Len, Index, FromBytes};
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;

    use timely::Container;
    impl<C: Columnar> Container for Column<C> {
        fn len(&self) -> usize {
            match self {
                Column::Typed(t) => t.len(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).len(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)).len(),
            }
        }
        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => *self = Column::Typed(Default::default()),
                Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }

        type ItemRef<'a> = C::Ref<'a>;
        type Iter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn iter<'a>(&'a self) -> Self::Iter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_iter(),
            }
        }

        type Item<'a> = C::Ref<'a>;
        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_iter(),
            }
        }
    }

    use timely::container::SizableContainer;
    impl<C: Columnar> SizableContainer for Column<C> {
        fn at_capacity(&self) -> bool {
            match self {
                Self::Typed(t) => {
                    let length_in_bytes = Indexed::length_in_bytes(&t.borrow());
                    length_in_bytes >= (1 << 20)
                },
                Self::Bytes(_) => true,
                Self::Align(_) => true,
            }
        }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
    }

    use timely::container::PushInto;
    impl<C: Columnar, T> PushInto<T> for Column<C> where C::Container: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            use columnar::Push;
            match self {
                Column::Typed(t) => t.push(item),
                Column::Align(_) | Column::Bytes(_) => {
                    // We really oughtn't be calling this in this case.
                    // We could convert to owned, but need more constraints on `C`.
                    unimplemented!("Pushing into Column::Bytes without first clearing");
                }
            }
        }
    }

    use timely::dataflow::channels::ContainerBytes;
    impl<C: Columnar> ContainerBytes for Column<C> {
        fn from_bytes(bytes: timely::bytes::arc::Bytes) -> Self {
            // Our expectation / hope is that `bytes` is `u64` aligned and sized.
            // If the alignment is borked, we can relocate. IF the size is borked,
            // not sure what we do in that case.
            assert!(bytes.len() % 8 == 0);
            if let Ok(_) = bytemuck::try_cast_slice::<_, u64>(&bytes) {
                Self::Bytes(bytes)
            }
            else {
                // println!("Re-locating bytes for alignment reasons");
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                Self::Align(alloc.into())
            }
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Column::Typed(t) => Indexed::length_in_bytes(&t.borrow()),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
            }
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => Indexed::write(writer, &t.borrow()).unwrap(),
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
            }
        }
    }

    impl<'a, C: Columnar> Index for &'a Column<C> {
        type Ref = C::Ref<'a>;

        fn get(&self, index: usize) -> Self::Ref {
            match self {
                Column::Typed(t) => t.borrow().get(index),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).get(index),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).get(index),
            }
        }
    }
}


pub use builder::ColumnBuilder;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::Update;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

mod builder {
    use std::collections::VecDeque;

    use columnar::{Columnar, Clear, Len, Push};
    use columnar::bytes::{EncodeDecode, Indexed};

    use super::Column;

    /// A container builder for `Column<C>`.
    pub struct ColumnBuilder<C: Columnar> {
        /// Container that we're writing to.
        current: C::Container,
        /// Empty allocation.
        empty: Option<Column<C>>,
        /// Completed containers pending to be sent.
        pending: VecDeque<Column<C>>,
    }

    use timely::container::PushInto;
    impl<C: Columnar, T> PushInto<T> for ColumnBuilder<C> where C::Container: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            // If there is less than 10% slop with 2MB backing allocations, mint a container.
            use columnar::Container;
            let words = Indexed::length_in_words(&self.current.borrow());
            let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
            if round - words < round / 10 {
                let mut alloc = Vec::with_capacity(words);
                Indexed::encode(&mut alloc, &self.current.borrow());
                self.pending.push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
            }
        }
    }

    impl<C: Columnar> Default for ColumnBuilder<C> {
        fn default() -> Self {
            ColumnBuilder {
                current: Default::default(),
                empty: None,
                pending: Default::default(),
            }
        }
    }

    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
    impl<C: Columnar> ContainerBuilder for ColumnBuilder<C> where C::Container: Clone {
        type Container = Column<C>;

        #[inline]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(container) = self.pending.pop_front() {
                self.empty = Some(container);
                self.empty.as_mut()
            } else {
                None
            }
        }

        #[inline]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            if !self.current.is_empty() {
                use columnar::Container;
                let words = Indexed::length_in_words(&self.current.borrow());
                let mut alloc = Vec::with_capacity(words);
                Indexed::encode(&mut alloc, &self.current.borrow());
                self.pending.push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
            }
            self.empty = self.pending.pop_front();
            self.empty.as_mut()
        }
    }

    impl<C: Columnar> LengthPreservingContainerBuilder for ColumnBuilder<C> where C::Container: Clone { }
}


/// A batcher for columnar storage.
pub type Col2ValBatcher<K, V, T, R> = MergeBatcher<Column<((K,V),T,R)>, batcher::ColumnChunker<Column<((K, V), T, R)>>, ColumnMerger<(K, V),T,R>>;
pub type Col2KeyBatcher<K, T, R> = Col2ValBatcher<K, (), T, R>;

/// Types for consolidating, merging, and extracting columnar update collections.
pub mod batcher {

    use std::collections::VecDeque;
    use columnar::Columnar;
    use timely::Container;
    use timely::container::{ContainerBuilder, PushInto};
    use differential_dataflow::difference::Semigroup;
    use crate::columnar::Column;
    // First draft: build a "chunker" and a "merger".

    #[derive(Default)]
    pub struct ColumnChunker<C> {
        /// Buffer into which we'll consolidate.
        ///
        /// Also the buffer where we'll stage responses to `extract` and `finish`.
        /// When these calls return, the buffer is available for reuse.
        empty: C,
        /// Consolidated buffers ready to go.
        ready: VecDeque<C>,
    }

    impl<C: Container + Clone + 'static> ContainerBuilder for ColumnChunker<C> {
        type Container = C;

        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(ready) = self.ready.pop_front() {
                self.empty = ready;
                Some(&mut self.empty)
            } else {
                None
            }
        }

        fn finish(&mut self) -> Option<&mut Self::Container> {
            self.extract()
        }
    }

    impl<'a, D, T, R, C2> PushInto<&'a mut Column<(D, T, R)>> for ColumnChunker<C2>
    where
        D: Columnar,
        for<'b> D::Ref<'b>: Ord + Copy,
        T: Columnar,
        for<'b> T::Ref<'b>: Ord + Copy,
        R: Columnar + Semigroup + for<'b> Semigroup<R::Ref<'b>>,
        for<'b> R::Ref<'b>: Ord,
        C2: Container + for<'b> PushInto<&'b (D, T, R)>,
    {
        fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {

            // Scoped to let borrow through `permutation` drop.
            {
                // Sort input data
                // TODO: consider `Vec<usize>` that we retain, containing indexes.
                let mut permutation = Vec::with_capacity(container.len());
                permutation.extend(container.drain());
                permutation.sort();

                self.empty.clear();
                // Iterate over the data, accumulating diffs for like keys.
                let mut iter = permutation.drain(..);
                if let Some((data, time, diff)) = iter.next() {

                    let mut owned_data = D::into_owned(data);
                    let mut owned_time = T::into_owned(time);

                    let mut prev_data = data;
                    let mut prev_time = time;
                    let mut prev_diff = <R as Columnar>::into_owned(diff);

                    for (data, time, diff) in iter {
                        if (&prev_data, &prev_time) == (&data, &time) {
                            prev_diff.plus_equals(&diff);
                        }
                        else {
                            if !prev_diff.is_zero() {
                                D::copy_from(&mut owned_data, prev_data);
                                T::copy_from(&mut owned_time, prev_time);
                                let tuple = (owned_data, owned_time, prev_diff);
                                self.empty.push_into(&tuple);
                                owned_data = tuple.0;
                                owned_time = tuple.1;
                            }
                            prev_data = data;
                            prev_time = time;
                            prev_diff = <R as Columnar>::into_owned(diff);
                        }
                    }

                    if !prev_diff.is_zero() {
                        D::copy_from(&mut owned_data, prev_data);
                        T::copy_from(&mut owned_time, prev_time);
                        let tuple = (owned_data, owned_time, prev_diff);
                        self.empty.push_into(&tuple);
                    }
                }
            }

            if !self.empty.is_empty() {
                self.ready.push_back(std::mem::take(&mut self.empty));
            }
        }
    }
}

pub use merger::ColumnMerger;
use crate::columnar::batcher::ColumnChunker;

mod merger {
    use columnar::{Columnar, Index};
    use timely::Container;
    use timely::progress::{Antichain, frontier::AntichainRef, Timestamp};
    use differential_dataflow::difference::Semigroup;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::trace::implementations::{BatchContainer, BuilderInput};
    use differential_dataflow::trace::implementations::merge_batcher::container::{ContainerMerger, ContainerQueue, MergerChunk};

    use crate::columnar::Column;

    /// A `Merger` implementation backed by `TimelyStack` containers (columnation).
    pub type ColumnMerger<D, T, R> = ContainerMerger<Column<(D,T,R)>,ColumnQueue<(D, T, R)>>;

    /// TODO
    pub struct ColumnQueue<T: Columnar> {
        list: Column<T>,
        head: usize,
    }

    impl<D: Columnar, T: Columnar, R: Columnar> ContainerQueue<Column<(D, T, R)>> for ColumnQueue<(D, T, R)>
    where
        for<'a> D::Ref<'a>: Ord,
        for<'a> T::Ref<'a>: Ord,
    {
        fn next_or_alloc(&mut self) -> Result<(D::Ref<'_>, T::Ref<'_>, R::Ref<'_>), Column<(D, T, R)>> {
            if self.is_empty() {
                Err(std::mem::take(&mut self.list))
            }
            else {
                Ok(self.pop())
            }
        }
        fn is_empty(&self) -> bool {
            self.head == self.list.len()
        }
        fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
            let (data1, time1, _) = self.peek();
            let (data2, time2, _) = other.peek();
            (data1, time1).cmp(&(data2, time2))
        }
        fn from(list: Column<(D, T, R)>) -> Self {
            ColumnQueue { list, head: 0 }
        }
    }

    impl<T: Columnar> ColumnQueue<T>
    // where
    //     for<'a> &'a T: Index,
    {
        fn pop(&mut self) -> T::Ref<'_> {
            self.head += 1;
            (&self.list).get(self.head - 1)
            // &self.list[self.head - 1]
        }

        fn peek(&self) -> T::Ref<'_> {
            (&self.list).get(self.head)
        }
    }

    impl<D, T, R> MergerChunk for Column<(D, T, R)>
    where
        D: Ord + Columnar + 'static,
        T: Ord + timely::PartialOrder + Clone + Columnar + 'static,
        for<'a> T::Ref<'a>: Copy,
        R: Default + Semigroup + Columnar + 'static
    {
        type TimeOwned = T;
        type DiffOwned = R;

        fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool {
            // TODO: Allocating, bad!
            let owned_time = Columnar::into_owned(*time);
            if upper.less_equal(&owned_time) {
                frontier.insert(owned_time);
                true
            }
            else { false }
        }
        fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, stash: &mut Self::DiffOwned) {
            let (data, time, diff1) = item1;
            let (_data, _time, diff2) = item2;
            Columnar::copy_from(stash, diff1);
            // TODO: Bad! Allocating!
            let diff2 = Columnar::into_owned(diff2);
            stash.plus_equals(&diff2);
            if !stash.is_zero() {
                self.push((data, time, &*stash));
            }
        }
        fn account(&self) -> (usize, usize, usize, usize) {
            let (size, capacity, allocations) = (0, 0, 0);
            // let cb = |siz, cap| {
            //     size += siz;
            //     capacity += cap;
            //     allocations += 1;
            // };
            // self.heap_size(cb);
            (self.len(), size, capacity, allocations)
        }
    }

    impl<K,V,T,R> BuilderInput<K, V> for Column<((K::Owned, V::Owned), T, R)>
    where
        K: BatchContainer,
        for<'a> K::ReadItem<'a>: PartialEq<&'a K::Owned>,
        K::Owned: Ord + Columnar + Clone + 'static,
        for<'a> <K::Owned as Columnar>::Ref<'a>: Ord + Copy,
        V: BatchContainer,
        for<'a> V::ReadItem<'a>: PartialEq<&'a V::Owned>,
        V::Owned: Ord + Columnar + Clone + 'static,
        for<'a> <V::Owned as Columnar>::Ref<'a>: Ord + Copy,
        T: Timestamp + Lattice + Columnar + Clone + 'static,
        R: Ord + Clone + Semigroup + Columnar + 'static,
    {
        type Key<'a> = <K::Owned as Columnar>::Ref<'a>;
        type Val<'a> = <V::Owned as Columnar>::Ref<'a>;
        type Time = T;
        type Diff = R;

        fn into_parts<'a>(((key, val), time, diff): Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
            (key, val, Columnar::into_owned(time), Columnar::into_owned(diff))
        }

        fn key_eq(this: &<K::Owned as Columnar>::Ref<'_>, other: K::ReadItem<'_>) -> bool {
            // TODO: BAD!
            let this: K::Owned = Columnar::into_owned(*this);
            K::reborrow(other) == &this
        }

        fn val_eq(this: &<V::Owned as Columnar>::Ref<'_>, other: V::ReadItem<'_>) -> bool {
            // TODO: BAD!
            let this: V::Owned = Columnar::into_owned(*this);
            V::reborrow(other) == &this
        }

        fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize) {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            let mut prev_keyval = None;
            for link in chain.iter() {
                for ((key, val), _, _) in link.iter() {
                    if let Some((p_key, p_val)) = prev_keyval {
                        if p_key != key {
                            keys += 1;
                            vals += 1;
                        } else if p_val != val {
                            vals += 1;
                        }
                    } else {
                        keys += 1;
                        vals += 1;
                    }
                    upds += 1;
                    prev_keyval = Some((key, val));
                }
            }
            (keys, vals, upds)
        }
    }

}

/// A layout based on columns
pub struct CStack<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

/// A trace implementation backed by columnar storage.
pub type ColumnKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<CStack<((K,()),T,R)>>>>;
/// A batcher for columnar storage
pub type ColumnKeyBatcher<K, T, R> = MergeBatcher<Column<((K,()),T,R)>, ColumnChunker<((K, ()), T, R)>, ColumnMerger<(K, ()),T,R>>;
/// A builder for columnar storage
pub type ColumnKeyBuilder<K, T, R> = RcBuilder<OrdKeyBuilder<CStack<((K,()),T,R)>, Column<((K,()),T,R)>>>;

mod batch {
    use columnar::{Columnar, Index};
    use differential_dataflow::IntoOwned;
    use differential_dataflow::trace::implementations::{BatchContainer, Layout, OffsetList, Update};
    use crate::columnar::{CStack, Column};

    impl<U: Update> Layout for CStack<U>
    where
        U::Key: columnar::Columnar,
        for<'a> <U::Key as Columnar>::Ref<'a>: Copy + Ord + IntoOwned<'a, Owned = U::Key>,
        <U::Key as Columnar>::Container: columnar::Push<U::Key>,
        U::Val: columnar::Columnar,
        for<'a> <U::Val as Columnar>::Ref<'a>: Copy + Ord + IntoOwned<'a, Owned = U::Val>,
        U::Time: columnar::Columnar,
        for<'a> <U::Time as Columnar>::Ref<'a>: Copy + Ord + IntoOwned<'a, Owned = U::Time>,
        <U::Time as Columnar>::Container: columnar::Push<U::Time>,
        U::Diff: columnar::Columnar + Ord,
        for<'a> <U::Diff as Columnar>::Ref<'a>: Copy + Ord + IntoOwned<'a, Owned = U::Diff>,
        <U::Diff as Columnar>::Container: columnar::Push<U::Diff>,
    {
        type Target = U;
        type KeyContainer = Column<U::Key>;
        type ValContainer = Column<U::Val>;
        type TimeContainer = Column<U::Time>;
        type DiffContainer = Column<U::Diff>;
        type OffsetContainer = OffsetList;
    }

    // The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
    // be presented with the actual contained type, rather than a type that borrows into it.
    impl<T> BatchContainer for Column<T>
    where
        T: Clone + Ord + Columnar + 'static,
        for<'a> T::Ref<'a>: Copy + Ord + IntoOwned<'a, Owned = T>,
    {
        type Owned = T;
        type ReadItem<'a> = T::Ref<'a>;

        fn with_capacity(_size: usize) -> Self {
            Self::default()
        }

        fn merge_capacity(_cont1: &Self, _cont2: &Self) -> Self {
            // let mut new = Self::default();
            // new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
            // new
            Self::default()
        }
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            (&self).get(index)
        }
        fn len(&self) -> usize {
            timely::container::Container::len(self)
        }
    }
}

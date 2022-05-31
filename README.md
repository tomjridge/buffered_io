# buffered_io, similar to OCaml's channels

OCaml already has channels. An `out_channel` is a file descriptor + buffer; the buffer is
filled then flushed to the end of the underlying file. An `in_channel` is a file
descriptor + buffer; the buffer is filled from the file, and reads come from the buffer;
when the buffer is fully read, it is refilled from the file.

By default, an `out_channel` writes to the end of a file, and an `in_channel` reads from
the front of a file. It may be possible to reposition these channels (eg so that the
`in_channel` reads from the middle of the file). However, the two types of channels are
independent: opening an `in_channel` and an `out_channel` on the same file must be handled
with care because the channels don't know anything about each other. So the notion of a
file as a "read/write" object has been lost: you either have the `in_channel` for reads,
or the `out_channel` for writes, and random access is maybe possible, but frowned on.

The aim of this code is to add read and write buffering on top of a file descriptor, in a
way that the read buffer and write buffer are aware of each other. The reason for doing
this is that often we need to buffer writes (in order to improve performance by avoiding
syscalls), but we still want random access to data in the file (and ideally this should be
buffered if possible). OCaml's existing channels do not make this possible: either you
work with an `in_channel` or an `out_channel`; it is difficult to work with both on the
same file descriptor.

## Simple approach which doesn't work

A simple approach would be to use an `out_channel` for the write buffering. For the read
buffering, if we are reading randomly we probably don't want to use a default `in_channel`
because the buffer size is 65536 = 2^16, which means that each random read (of, say, a few
bytes) will cause 2^16 bytes to be read. In applications such as Irmin, we typically read
a small number of bytes for an object's header (for example, 42 bytes), and then we read
the object itself. Most objects are small, < 1024 bytes including header, so a good choice
of buffer size for reads might be 1024, which usually involves only 1 block read, but is
enough to avoid multiple reads to the same offset.

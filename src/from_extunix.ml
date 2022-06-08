(** These interfaces from extUnix are more or less "standard". Doc from extUnix included
    here to clarify restart nature etc. *)

(* extUnix pwrite versions:

[all_pwrite fd off buf ofs len] writes up to [len] bytes from file
    descriptor [fd] at offset [off] (from the start of the file) into
    the string [buf] at offset [ofs]. The file offset is not changed.

    [all_pwrite] repeats the write operation until all characters have
    been written or an error occurs. Returns less than the number of
    characters requested on EAGAIN, EWOULDBLOCK but never 0. Continues
    the write operation on EINTR. Raises an Unix.Unix_error exception
    in all other cases.

---

[pwrite fd off buf ofs len] writes up to [len] bytes from file
    descriptor [fd] at offset [off] (from the start of the file) into
    the string [buf] at offset [ofs]. The file offset is not changed.

    [pwrite] repeats the write operation until all characters have
    been written or an error occurs. Raises an Unix.Unix_error exception
    if 0 characters could be written before an error occurs. Continues
    the write operation on EINTR. Returns the number of characters
    written in all other cases.

---

(also intr_pwrite)

---

What is the difference between all_pwrite and pwrite? all_pwrite will return less than the
number of chars requested (but never 0) if EAGAIN or EWOULDBLOCK; pwrite will only repeat
on EINTR, otherwise will return the number of chars (even if an exception in the
underlying operation), unless there is an exception before 0 chars could be written...

so pwrite is probably the one to use

*)



(* the following pwrite is not the POSIX pwrite - there is an attempt to continue on EINTR
   - but probably the most useful version of pwrite that people would want to use.. it
   saves manually repeating the pwrite. The use of "string" rather than "bytes" means that
   we can't be sure that somehow the immutability isn't being used... but probably it
   isn't and we are safe to "Bytes.unsafe_to_string buf" before calling pwrite.  *)

(** [pwrite fd off buf ofs len] writes up to [len] bytes from file
    descriptor [fd] at offset [off] (from the start of the file) into
    the string [buf] at offset [ofs]. The file offset is not changed.

    [pwrite] repeats the write operation until all characters have
    been written or an error occurs. Raises an Unix.Unix_error exception
    if 0 characters could be written before an error occurs. Continues
    the write operation on EINTR. Returns the number of characters
    written in all other cases. *)
let pwrite : Unix.file_descr -> int -> string -> int -> int -> int = ExtUnix.All.pwrite

(* OK, so if we wanted to simplify this, what would we do? It is fairly usual to slice the
   bytes buf, so we can use a single buf argument; this gives us... *)

(** [pwrite] repeats the write operation until all characters have
    been written or an error occurs. Raises an Unix.Unix_error exception
    if 0 characters could be written before an error occurs. Continues
    the write operation on EINTR. Returns the number of characters
    written in all other cases. *)
let pwrite fd off buf : int =
  let buf = Bytes.unsafe_to_string buf in
  pwrite fd off buf 0 (String.length buf)


(** [pread fd off buf ofs len] reads up to [len] bytes from file
    descriptor [fd] at offset [off] (from the start of the file) into
    the string [buf] at offset [ofs]. The file offset is not changed.

    [pread] repeats the read operation until all characters have
    been read or an error occurs. Raises an Unix.Unix_error exception
    if 0 characters could be read before an error occurs. Continues
    the read operation on EINTR. Returns the number of characters
    written in all other cases. *)
let pread : Unix.file_descr -> int -> bytes -> int -> int -> int = ExtUnix.All.pread

(* the simplification would presumably be... *)

let pread fd off buf : int = pread fd off buf 0 (Bytes.length buf)

(* 

sketch: use a normal fd, with a Buffer.t to buffer appends; use write when flushing the
buffer; use pread to read (so that it doesn't disturb the fd's position at the end of the
file); for pwrite, if the write is at the end of the file, then it is an append;
otherwise, if it is wholly before the buffered data, we can issue it as a pwrite,
otherwise we flush and pwrite.

Then there are two notions of size: the size on disk, and the size with the buffer (ie the
size on disk if we were to perform a flush first).

We should first implement an UNBUFFERED version on top of the normal OCaml file intfs.

*)

(** Essentially the Y combinator; useful for anonymous recursive
    functions. The k argument is the recursive callExample:

    {[
      iter_k (fun ~k n -> 
          if n = 0 then 1 else n * k (n-1))

    ]}


*)
let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x  


module type Desired_pread_pwrite_intf = sig
  
  type t

  (** [pwrite t ~off ~buf] writes buffer [buf] to [t] at offset [off]; writes all bytes,
      or throws an exception *)
  val pwrite: t -> off:int -> buf:bytes -> unit

  (** [pread t ~off ~buf] attempts to fill buffer [buf] with data from [t] at offset
      [off]; returns the number of bytes actually read; this may be less than the size of
      [buf] iff we are reading near the end of the file; throws an exception in
      exceptional circumstances *)
  val pread: t -> off:int -> buf:bytes -> int

end 

module Pread_pwrite : Desired_pread_pwrite_intf with type t=Unix.file_descr = struct
  type t = Unix.file_descr

  let pwrite t ~off ~buf = 
    let n = Pread_pwrite.pwrite t off buf in
    assert(n=Bytes.length buf);
    ()

  let pread t ~off ~buf = Pread_pwrite.pread t off buf
end

type pwrite_t = (off:int -> buf:bytes -> unit)

type pread_t = (off:int -> buf:bytes -> int)

module Copy = struct

  (* This copy assumes that there are [len] bytes to copy *)
  let copy ~(src:pread_t) ~src_off ~len ~(dst:pwrite_t) ~dst_off = 
    match len = 0 with
    | true -> ()
    | false -> 
      let pread,pwrite = src,dst in
      let buf_sz = 8192 in
      let buf0 = Bytes.create buf_sz in
      (src_off,dst_off,len) |> iter_k (fun ~k (src_off,dst_off,len) -> 
          match len = 0 with
          | true -> ()
          | false -> 
            let len' = min buf_sz len in
            let buf' = Bytes.sub buf0 0 len' in
            let n = pread ~off:src_off ~buf:buf' in
            assert(n>0);
            pwrite ~off:dst_off ~buf:(Bytes.sub buf' 0 n);
            k (src_off+n,dst_off+n,len-n))
end

module V1_using_extunix = struct

  type t = Unix.file_descr

  let create ~path =
    let ok = not (Sys.file_exists path) in
    assert(ok);
    let fd = Unix.(openfile path [O_CREAT;O_RDWR;O_EXCL;O_CLOEXEC] 0o660) in
    fd

  let open_ ~path =
    let ok = Sys.file_exists path in
    assert(ok);
    let fd = Unix.(openfile path [O_RDWR;O_CLOEXEC] 0o660) in
    fd

  (* read into buf, as much as we can; update off; return the number of bytes read; if
     this is less than |buf|, then we hit the end of the file; FIXME it seems weird to
     update off whilst also returning the length; perhaps prefer one or the other *)
  let pread fd ~off ~buf : int = Pread_pwrite.pread fd ~off ~buf

  let read_string fd ~off ~len = 
    let buf = Bytes.create len in
    let n = pread fd ~off ~buf in
    Bytes.unsafe_to_string (Bytes.sub buf 0 n)

  (* write from buf as much as we can; update off; assumes the entire buffer is written *)
  let pwrite fd ~off ~buf : unit = Pread_pwrite.pwrite fd ~off ~buf 

  (* NOTE calling stat is a syscall, which is somewhat costly *)
  let size fn = Unix.((stat fn).st_size)

  (* NOTE the problem with this implementation is that pwrite may have altered the length
     of the file, so the file descriptor no longer points at the end of the file; to avoid
     the call to lseek, we could: track the length of the file; track the position of the
     file descriptor; only lseek when necessary; alternatively we could enforce that a
     pwrite never extends a file (this is rather strange, but maybe justifiable);
     
     so perhaps better to maintain the file size explicitly?
 *)
  let append fd s = 
    (* nothing that follows will alter buf, so unsafe_of_string is safe *)
    let buf = Bytes.unsafe_of_string s in        
    (* ensure positioned at the end; NOTE that a file opened O_APPEND on Linux means that
       pwrites go to end of file! so we can't use O_APPEND with pwrite *)
    let off = Unix.lseek fd 0 SEEK_END in    
    pwrite fd ~off ~buf;
    ()
end


module V2_with_explicit_size = struct

  module V1 = V1_using_extunix

  type t = {fd:Unix.file_descr; mutable sz:int }

  let create ~path = 
    V1.create ~path |> fun fd ->
    {fd;sz=Unix.(lseek fd 0 SEEK_END)}

  let open_ ~path =
    V1.open_ ~path |> fun fd ->
    {fd;sz=Unix.(lseek fd 0 SEEK_END)}

  let pread t ~off ~buf = 
    V1.pread t.fd ~off ~buf

  let pwrite t ~off ~buf =
    V1.pwrite t.fd ~off ~buf;
    t.sz <- max (off + Bytes.length buf) t.sz;
    ()
  
  let size t = t.sz

  (* NOTE the following executes using a single pwrite system call *)
  let append t s : unit = pwrite t ~off:t.sz ~buf:(Bytes.unsafe_of_string s)
end

module V3_with_write_buffer = struct

  module V2 = V2_with_explicit_size

  (* FIXME the buf_pos is just the V2 size? ie the on-disk size? yes *)

  type t = { v2:V2.t; mutable buf_pos:int; buf:Buffer.t }

  (* pwrites beyond the on-disk size force a flush of the buffer *)

  (* we also have a limit on the buffer size *)

  let create ~path = 
    V2.create ~path |> fun v2 -> 
    { v2; buf_pos=V2.size v2; buf=Buffer.create 10}
                     
  let open_ ~path = 
    V2.open_ ~path |> fun v2 -> 
    { v2; buf_pos=V2.size v2; buf=Buffer.create 10}

  let flush t = 
    let s = Buffer.contents t.buf in
    Buffer.reset t.buf;
    V2.append t.v2 s;
    t.buf_pos <- t.buf_pos + String.length s;
    ()

  (* NOTE this is now the virtual size *)
  let virt_size t = t.buf_pos + Buffer.length t.buf

  let ondisk_size t = t.buf_pos

  let pread t ~off ~buf =
    match off + Bytes.length buf >= t.buf_pos with
    | true -> 
      flush t;
      V2.pread t.v2 ~off ~buf
    | false -> 
      (* can read from non-buffered part of file *)
      V2.pread t.v2 ~off ~buf

  let pwrite t ~off ~buf =
    match off + Bytes.length buf >= t.buf_pos with
    | true -> 
      flush t;
      V2.pwrite t.v2 ~off ~buf;
      (* may need to update buf_pos so it is positioned at end of file *)
      t.buf_pos <- max t.buf_pos (off + Bytes.length buf);
      ()
    | false -> 
      (* can pwrite to non-buffered part of file *)
      V2.pwrite t.v2 ~off ~buf;
      ()  

  let max_buf_sz = 1024*4096

  let append t s = 
    Buffer.add_string t.buf s;
    (* assumes Buffer.length is quick *)
    match Buffer.length t.buf >= max_buf_sz with
    | true -> flush t; ()
    | false -> ()

end


module V4_with_read_buffer = struct

  module V3 = V3_with_write_buffer

  type t = {
    v3 : V3.t;
    buf0 : bytes;
    empty_slice : bytes;
    mutable buf_pos : int;
    mutable buf : bytes;
  }
  (** buf0 is the original bytes buffer; empty_slice is the slice of buf0 from 0 to 0; buf
      is the slice of buf0 that is valid at buf_pos *)

  let default_buf_sz = 4096  

  let create ~path = 
    V3.create ~path |> fun v3 -> 
    let buf0 = Bytes.create default_buf_sz in
    let empty_slice = Bytes.sub buf0 0 0 in
    {v3; buf0; empty_slice; buf_pos=0; buf=empty_slice }

  let open_ ~path =
    V3.open_ ~path |> fun v3 -> 
    let buf0 = Bytes.create default_buf_sz in
    let empty_slice = Bytes.sub buf0 0 0 in
    {v3; buf0; empty_slice; buf_pos=0; buf=empty_slice }

  let flush t = V3.flush t.v3

  let virt_size t = V3.virt_size t.v3

  let ondisk_size t = V3.ondisk_size t.v3

  let invalidate_read_buffer t = 
    t.buf_pos <- (-1);
    t.buf <- t.empty_slice;
    ()

  let pwrite t ~off ~buf = 
    (* we have to check whether (off,buf) overlaps with the read buffer *)
    let read_buffer_empty = Bytes.length t.buf = 0 in
    let read_buffer_before = t.buf_pos + Bytes.length t.buf <= off in
    let read_buffer_after = t.buf_pos >= off + Bytes.length buf in
    match read_buffer_empty || read_buffer_before || read_buffer_after with
    | true -> 
      (* no overlap; do the pwrite *)
      V3.pwrite t.v3 ~off ~buf;
      ()
    | false -> 
      (* overlap with read buffer; we need to invalidate the read buffer after pwrite *)
      V3.pwrite t.v3 ~off ~buf;
      invalidate_read_buffer t;
      ()

  let pread t ~off ~buf =
    (* do we need to take account of the write buffer as well? *)
    (* check if we can service this from the existing buffer *)
    let len = Bytes.length buf in
    let within_buf = off >= t.buf_pos && off+len <= Bytes.length t.buf in
    match within_buf with
    | true -> 
      (* we can read from the buffer directly *)
      Bytes.blit t.buf (off - t.buf_pos) buf 0 len;
      len
    | false -> 
      (* we pread into buf0 at the given position *)
      let n = V3.pread t.v3 ~off ~buf:t.buf0 in
      t.buf_pos <- off;
      t.buf <- Bytes.sub t.buf0 0 n;
      (* then we blit from t.buf to the return buf *)
      let len = min n len in
      Bytes.blit t.buf 0 buf 0 len;
      (* and return the number read *)
      len

end


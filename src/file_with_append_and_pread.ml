(* 

NOTE supporting the append operation on files: the temptation is to use a file descriptor,
and implement append by ensuring that the fd position is always at the end of the file;
usually this works fine, but if we allow pread and pwrite, then it can be that pwrite
extends the file, so that the fd position is no longer at the end of the file; then append
requires an lseek to the end of the file; on Linux, if we try to use a file in O_APPEND
mode, then pwrites will automatically go to the end of the file

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
            (* NOTE the following Bytes.sub is unnecessary if we assume there are len
               bytes available, ie buf' has been filled *)
            pwrite ~off:dst_off ~buf:(Bytes.sub buf' 0 n);
            k (src_off+n,dst_off+n,len-n))
end

module V1_using_extunix = struct

  type t = Unix.file_descr

  let create path =
    let ok = not (Sys.file_exists path) in
    assert(ok);
    let fd = Unix.(openfile path [O_CREAT;O_RDWR;O_EXCL;O_CLOEXEC] 0o660) in
    fd

  let open_ path =
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
  let size fd = Unix.((fstat fd).st_size)

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

  let close t = Unix.close t
end

(** This code keeps track of the on-disk size internally, in order to know the size of the
    file (and the position to append) without seeking. *)
module V2_with_explicit_size = struct

  module V1 = V1_using_extunix

  type t = {fd:V1.t; mutable sz:int }

  let create path = 
    V1.create path |> fun fd ->
    {fd;sz=Unix.(lseek fd 0 SEEK_END)}

  let open_ path =
    V1.open_ path |> fun fd ->
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

  let close t = V1.close t.fd
end

(** At a write buffer, for appends at the end of the file (not for multiple adjacent
    pwrites - these are still separate syscalls) *)
module V3_with_write_buffer = struct

  module V2 = V2_with_explicit_size

  (** [default_max_wbuf] currently 4MiB *)
  let default_max_wbuf = 4*1024*1024

  type t = { v2:V2.t; wbuf:Buffer.t; mutable max_wbuf:int }

  let ondisk_size t = V2.size t.v2

  let virt_size t = ondisk_size t + Buffer.length t.wbuf

  (* expose write buffer, for testing *)
  let write_buffer_contents t = Buffer.contents t.wbuf

  (* The position of the end of the file, i.e. where the data in the buffer will go *)
  let buf_pos t = ondisk_size t

  let create ?(max_wbuf=default_max_wbuf) path = 
    V2.create path |> fun v2 -> 
    { v2; wbuf=Buffer.create 10; max_wbuf=max_wbuf}
                     
  let open_ ?(max_wbuf=default_max_wbuf) path = 
    V2.open_ path |> fun v2 -> 
    { v2; wbuf=Buffer.create 10; max_wbuf}

  let flush t = 
    let s = Buffer.contents t.wbuf in
    Buffer.reset t.wbuf;
    V2.append t.v2 s;
    ()

  let pread t ~off ~buf =
    let before_buf = off + Bytes.length buf <= buf_pos t in
    match before_buf with
    | false -> 
      flush t;
      V2.pread t.v2 ~off ~buf
    | true -> 
      (* can read from non-buffered part of file *)
      V2.pread t.v2 ~off ~buf

  (* pwrites beyond the on-disk size force a flush of the buffer *)
  let pwrite t ~off ~buf =
    let before_buf = off + Bytes.length buf <= buf_pos t in
    match before_buf with
    | false -> 
      flush t;
      V2.pwrite t.v2 ~off ~buf;
      ()
    | true -> 
      (* can pwrite to non-buffered part of file *)
      V2.pwrite t.v2 ~off ~buf;
      ()  

  let append t s = 
    Buffer.add_string t.wbuf s;
    (* assumes Buffer.length is quick *)
    match Buffer.length t.wbuf > t.max_wbuf with
    | true -> flush t; ()
    | false -> ()

  let close t = 
    flush t;
    V2.close t.v2;
    ()

end


(** With Irmin/Tezos, we often see the following pattern: read a small amount from offset;
    read a larger amount from offset, or from offset+delta, where delta is small
    (i.e. read a bit more just beyond offset). As separate system calls, this is more
    expensive than if we just read the larger amount initially, and cached it. This is
    what the following code attempts to do. At a given offset, we read at least n bytes,
    and store them in a cache; then we attempt to service further preads from that
    cache. *)
module V4_with_read_buffer = struct

  module V3 = V3_with_write_buffer

  type t = {
    v3 : V3.t;
    buf0 : bytes;
    empty_slice : bytes;
    mutable buf_pos : int;
    mutable rbuf : bytes;
  }
  (** buf0 is the original bytes buffer; empty_slice is the slice of buf0 from 0 to 0; buf
      is the slice of buf0 that is valid at buf_pos *)

  (** [default_rbuf_sz] currently 4096 *)
  let default_rbuf_sz = 4096  

  let virt_size t = V3.virt_size t.v3

  let ondisk_size t = V3.ondisk_size t.v3

  let debug_write_buffer_contents t = V3.write_buffer_contents t.v3

  (* expose read buffer, for testing *)
  let debug_read_buffer t = (t.buf_pos,t.rbuf)

  let create ?(max_wbuf=V3.default_max_wbuf) ?(rbuf_sz=default_rbuf_sz) path = 
    V3.create ~max_wbuf path |> fun v3 -> 
    let buf0 = Bytes.create rbuf_sz in
    let empty_slice = Bytes.sub buf0 0 0 in
    {v3; buf0; empty_slice; buf_pos=0; rbuf=empty_slice }

  let open_ ?(max_wbuf=V3.default_max_wbuf) ?(rbuf_sz=default_rbuf_sz) path =
    V3.open_ ~max_wbuf path |> fun v3 -> 
    let buf0 = Bytes.create rbuf_sz in
    let empty_slice = Bytes.sub buf0 0 0 in
    {v3; buf0; empty_slice; buf_pos=0; rbuf=empty_slice }

  let flush t = V3.flush t.v3

  let invalidate_read_buffer t = 
    t.buf_pos <- (-1);
    t.rbuf <- t.empty_slice; (* i.e., read_buffer_empty is true *)
    ()

  let pwrite t ~off ~buf = 
    (* we have to check whether (off,buf) overlaps with the read buffer *)
    let read_buffer_empty = Bytes.length t.rbuf = 0 in
    let read_buffer_before = t.buf_pos + Bytes.length t.rbuf <= off in
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
    let within_buf = off >= t.buf_pos && off+len <= t.buf_pos + Bytes.length t.rbuf in
    match within_buf with
    | true -> 
      (* we can read from the buffer directly *)
      Bytes.blit t.rbuf (off - t.buf_pos) buf 0 len;
      len
    | false -> 
      (* is the amount of data small? *)
      match len <= Bytes.length t.buf0 with
      | false -> 
        (* a large read, which we can't service from the cache; so just go to disk *)
        let n = V3.pread t.v3 ~off ~buf in
        n
      | true -> 
        (* we pread into t.buf0 at the given position; NOTE that V3.pread will flush the
           write buffer if required *)
        let n = V3.pread t.v3 ~off ~buf:t.buf0 in
        t.buf_pos <- off;
        t.rbuf <- Bytes.sub t.buf0 0 n;
        (* then we blit from t.buf to the return buf *)
        let len = min n len in
        Bytes.blit t.rbuf 0 buf 0 len;
        (* and return the number read *)
        len

  let append t s = V3.append t.v3 s

  let close t = 
    V3.close t.v3;
    ()

end

(** Restricted interface exposed by {!V4_with_read_buffer} *)
module type S4 = sig
  type t
  val virt_size : t -> int
  val ondisk_size : t -> int
  val debug_write_buffer_contents : t -> string
  val debug_read_buffer : t -> int * bytes
  val create : ?max_wbuf:int -> ?rbuf_sz:int -> string -> t
  val open_ : ?max_wbuf:int -> ?rbuf_sz:int -> string -> t
  val flush : t -> unit
  val pwrite : t -> off:int -> buf:bytes -> unit
  val pread : t -> off:int -> buf:bytes -> int
  val append : t -> string -> unit
  val close: t -> unit
end

module _ : S4 = V4_with_read_buffer

module Test = struct

  let fn = "./test.tmp"

  let test_v1 () =
    let open V1_using_extunix in
    (try Sys.remove fn with _ -> ());
    let t = create fn in
    let hello = "hello" in
    append t hello;
    let buf = Bytes.create 100 in
    let n = pread t ~off:0 ~buf in
    assert(n=String.length hello);
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello);
    let world = " world!" in
    append t world;
    let n = pread t ~off:0 ~buf in
    assert(n=String.length (hello^world));
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello^world);
    assert(size t = n);
    Printf.printf "%s: test_v1 complete\n%!" __FILE__;
    ()

  let test_v2 () = 
    let open V2_with_explicit_size in
    (try Sys.remove fn with _ -> ());
    let t = create fn in
    let hello = "hello" in
    append t hello;
    let buf = Bytes.create 100 in
    let n = pread t ~off:0 ~buf in
    assert(n=String.length hello);
    assert(size t = n);    
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello);
    let world = " world!" in
    append t world;
    let n = pread t ~off:0 ~buf in
    assert(n=String.length (hello^world));
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello^world);
    assert(size t = n);    
    Printf.printf "%s: test_v2 complete\n%!" __FILE__;
    ()
  
  let test_v3 () = 
    let open V3_with_write_buffer in
    (try Sys.remove fn with _ -> ());
    let t = create ~max_wbuf:5 fn in
    let hello = "hello" in
    append t hello;
    assert(write_buffer_contents t = hello);
    assert(ondisk_size t = 0);
    assert(virt_size t = String.length hello);       
    let buf = Bytes.create 100 in
    (* the following pread forces the flush of the hello *)
    let n = pread t ~off:0 ~buf in
    assert(n=String.length hello);
    assert(write_buffer_contents t = "");
    assert(ondisk_size t = n);
    assert(virt_size t = n);
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello);
    let world = " world!" in
    (* following will be automatically flushed *)
    append t world;
    let n = pread t ~off:0 ~buf in
    assert(n=String.length (hello^world));
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello^world);
    assert(ondisk_size t = n);    
    assert(virt_size t = n);
    Printf.printf "%s: test_v3 complete\n%!" __FILE__;
    ()

  let test_v3_pread_before () = 
    let open V3_with_write_buffer in
    (try Sys.remove fn with _ -> ());
    let t = create ~max_wbuf:5 fn in
    let hello = "hello" in
    append t hello;
    flush t;
    let n = String.length hello in
    assert(write_buffer_contents t = "");
    assert(ondisk_size t = n);
    assert(virt_size t = n);
    let wor = " wor" in
    (* following will not be automatically flushed *)
    append t wor;
    let buf = Bytes.create 5 in
    (* following should read the hello, without causing the flush of wor *)
    let n = pread t ~off:0 ~buf in
    assert(n=String.length (hello));
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello);
    assert(ondisk_size t = n);    
    assert(virt_size t = n+String.length wor);
    Printf.printf "%s: test_v3_pread_before complete\n%!" __FILE__;
    ()

  let test_v4 () = 
    let open V4_with_read_buffer in
    (try Sys.remove fn with _ -> ());
    let t = create ~max_wbuf:5 ~rbuf_sz:5 fn in
    let hello = "hello" in
    append t hello;
    flush t;
    let n = String.length hello in
    assert(debug_write_buffer_contents t = "");
    assert(ondisk_size t = n);
    assert(virt_size t = n);
    let wor = " wor" in
    (* following will *not* be automatically flushed *)
    append t wor;
    let buf = Bytes.create 5 in
    (* following should read the hello, without causing the flush of wor *)
    let n = pread t ~off:0 ~buf in
    assert(n=String.length (hello));
    assert(Bytes.sub buf 0 n |> Bytes.to_string = hello);
    assert(ondisk_size t = n);    
    assert(virt_size t = n+String.length wor);
    Printf.printf "%s: test_v4 complete\n%!" __FILE__;
    ()
        
  let test () = test_v1 (); test_v2 (); test_v3 (); test_v3_pread_before (); test_v4 (); ()

end

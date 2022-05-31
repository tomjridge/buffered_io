(** Buffered io *)


(** Common pread/pwrite interfaces *)

module Pwrite = struct
  type fun_t = off:int ref -> bytes -> unit
  type t = {
    pwrite: off:int ref -> bytes -> unit;
  }
end

module Pread = struct
  let fun_t = off:int ref -> len:int -> buf:bytes -> int
  type t = {
    pread : off:int ref -> len:int -> buf:bytes -> int; 
  }
end

module File = struct
  type t = {
    pread: Pread.fun_t;
    pwrite:Pwrite.fun_t;
    size: unit -> int;
    fsync: unit -> unit;
  }
end

module Write_buffer = struct
  let const_INITIAL_BUF_SIZE = 65536
  let const_MAX_BUF_SIZE = 20 * 1024 * 1024
  type t = {
    start:int ref; (* start position in underlying file *)
    buf: Buffer.t; (* buffer of things that need to be appended from start onwards *)
  }
  let create ~start = { start; buf=Buffer.create const_INITIAL_BUF_SIZE }
end

module Read_buffer = struct
  let const_INITIAL_BUF_SIZE = 1024
  type t = {
    buf: bytes;
    start:int ref; 
    valid:int ref;
  }
  let create () = { buf=Bytes.create const_INITIAL_BUF_SIZE; start=0; valid=0 }
end


type buffered = {
  underlying: File.t;
  wbuf: Write_buffer.t;
  rbuf: Read_buffer.t;
}
  
let create underlying =
  {
    underlying;
    wbuf=(
      let start = File.size underlying in
      Write_buffer.create ~start);
    rbuf=Read_buffer.create ()
  }


(* flush the write buffer *)
let flush (t:buffered) = 
  let buf = t.wbuf.buf |> Buffer.to_bytes in
  let len = Bytes.length buf in
  let n = t.underlying.pwrite ~off:(ref !t.wbuf.start) ~len:(Bytes.length buf) ~buf in
  assert(n=len);
  
let maybe_flush (t:buffered) = 
  match t.wbuf.buf |> Buffer.length > Write_buffer.const_MAX_BUF_SIZE with
  | true -> flush t
  | false -> ()

let pwrite (t:buffered) ~off ~len ~buf =
  assert(len < Bytes.length buf);
  (* first check if the write is "large" in which case just flush what we have and submit
     the large write to the underlying file *)
  let is_large = len > Write_buffer.const_MAX_BUF_SIZE in
  match is_large with
  | true -> (
      flush t;
      let n = t.underlying.pwrite ~off ~len ~buf in
      n)
  | false -> begin
      (* check if we can append to the buffer *)
      match t.wbuf.start + Buffer.length t.wbuf.buf = !off with
      | true -> 
        Buffer.add_subbytes t.wbuf.buf buf 0 len;
        maybe_flush t;
        ()
      | false -> 
        flush t;
        t.wbuf.start := !off;
        Buffer.add_subbytes t.wbuf.buf buf 0 len;
        (* should we first check to see if we are attempting a huge pwrite? in which case,
           don't bother to buffer just write out directly *)
        ()
    end

let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x  


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

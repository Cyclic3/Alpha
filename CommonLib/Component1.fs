namespace Cyclic3.Alpha
open Swensen.Unquote
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Compiler.Interactive.Shell
type SerializableObject(v:obj) = 
    new(b:System.Runtime.Serialization.SerializationInfo,_) = SerializableObject(b.GetValue("",typeof<obj>))
    member x.Value = v
    interface System.Runtime.Serialization.ISerializable with member x.GetObjectData(info,_) = info.AddValue("",v)
    static member OfBytes b = let m = new System.IO.MemoryStream(b:byte[]) in let bf = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter() in bf.Deserialize(m)
[<AutoOpen>]
module Core = 
    let getcompiler()=FsiEvaluationSession.Create(FsiEvaluationSession.GetDefaultConfiguration(),[|""|],System.IO.TextReader.Null,System.IO.TextWriter.Null,System.IO.TextWriter.Null)
    let compile<'T> (compiler:FsiEvaluationSession) code = compiler.EvalExpression(code)|>Option.map(fun(v:FsiValue) -> v.ReflectionValue|>unbox<'T>)
    let run (compiler:FsiEvaluationSession) code = compiler.EvalInteraction code
    let multi() = 
        let rec inner prev = 
            printf "> "
            let v = System.Console.ReadLine()
            match v.IndexOf(";;") with
            | -1 -> inner(prev+"\n"+v)
            | 0 -> prev
            | i -> prev + "\n" + v.[..i-1]
        inner("")
    open System.IO.Compression
    let compress b = 
        let m = new System.IO.MemoryStream()
        let s = new DeflateStream(m,CompressionMode.Compress)
        s.Write(b,0,b.Length)
        s.Close()
        m.ToArray()
    let decompress (b:byte[]) = 
        let m = new System.IO.MemoryStream(b)
        let s = new DeflateStream(m,CompressionMode.Decompress)
        let m' = new System.IO.MemoryStream()
        s.CopyTo m'
        m'.ToArray()
    let SHA3 length = new SHA3.SHA3Unmanaged(length)
    let hash b length = SHA3(length).ComputeHash(b:byte[])
    let makeserializable o = new SerializableObject(o)
    let getbytes(o:#System.Runtime.Serialization.ISerializable)=
        let s=new System.IO.MemoryStream()
        (new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()).Serialize(s,o)
        s.ToArray()
    let randinseq s = let r = new System.Random() in Seq.item (r.Next(Seq.length s))
type Process private(t:System.Threading.ParameterizedThreadStart,guid:System.Guid,parent:System.Guid option,control) =
    let r : obj option ref = ref None
    let mutable thread = new System.Threading.Thread(t)
    new(t:System.Threading.ParameterizedThreadStart,?parent,?control) = new Process(t,System.Guid.NewGuid(),defaultArg parent None,defaultArg control false)
    new(b:System.Runtime.Serialization.SerializationInfo,_) = let s,g1,g2,c = b.GetValue("",typeof<System.Threading.ParameterizedThreadStart*System.Guid*System.Guid option*bool>)|>unbox in Process(s,g1,g2,c)
    //new(e:Expr) = Process(System.Threading.ParameterizedThreadStart(fun (r) -> let r' = r:?>obj option ref in r':=(Some (e.Eval()))))
    member x.IsControl = control
    member x.Guid() = guid
    member x.Parent() = parent
    member x.Start() = thread.Start(r)
    member x.Stop() = thread.Abort()
    member x.Wait() = thread.Join()
    member x.Result = r.Value
    member x.GetResult() =
        x.Wait()    
        x.Stop()
        x.Result.Value
    member x.GetBytes() = let m = new System.IO.MemoryStream() in let b = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter() in b.Serialize(m,x);m.ToArray()
    member x.GetSerializableResult() = SerializableObject(x.GetResult())
    interface System.Runtime.Serialization.ISerializable with member x.GetObjectData(info,_) = info.AddValue("",(t,guid,parent,control))
    static member OfBytes(b:byte[]) = let m = new System.IO.MemoryStream(b) in let b = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter() in b.Deserialize(m)|>unbox<Process>
    static member OfAsync(a:Async<'a>) = 
        Process(System.Threading.ParameterizedThreadStart(fun (r) -> let r' = r:?>obj option ref in r':=(Some(Async.RunSynchronously a|>box))))
    static member OfExpr(e:Expr,?parent,?control) = 
        Process(System.Threading.ParameterizedThreadStart(fun (r) -> let r' = r:?>obj option ref in r':=(Some (e.Eval()|>box))),parent = p)
module Node =
    open System.Net
    open System.Net.Sockets
    open System.Security.Cryptography
    ///When this is returned to a Slave, and Process.control = true, the slave with run this with itself as the parameter
    type DistributedJob = {j:Process[]}
    type RemoteControl = {f:Slave -> obj}
    and Slave(argv) = 
        let help () = ()
        let args = Array.append argv [|"--"|]|> Seq.pairwise |> Map.ofSeq
        let getarg s = match Array.tryPick(fun s' -> args.TryFind s') s with |Some(v) -> v |None -> printfn "Option %s requires a parameter" s.[0];exit -3
        let hasflags f = Array.exists(fun s' -> args.ContainsKey s') f
        do if hasflags [|"-h";"--help";"/?"|] then help();exit 0
        let warn (m:string) = printfn "WARNING: %s" (m.ToUpper())
        do if not (hasflags [|"-u";"--uri"|]) then printfn "Requires stream uri (-u or --uri)";exit -1
        let istream,ostream =
            let uri = getarg [|"-u";"--uri"|]
            if hasflags [|"-t";"--tcp"|] then
                let ip,port = if hasflags [|"-6";"--ipv6"|] then let p = uri.IndexOf(']') in uri.[1..p-1],uri.[p+2..] else let p = uri.Split(':') in p.[0],p.[1]
                let c = (new TcpClient(ip,int port))
                (fun b -> c.GetStream().Write(b,0,b.Length)),(fun()-> while not(c.GetStream().DataAvailable)do()done;let b=Array.zeroCreate c.Available in c.GetStream().Read(b,0,b.Length)|>ignore;b)
            elif hasflags [|"-p";"--pipe"|] then 
                let pipe = new System.IO.Pipes.NamedPipeClientStream(uri) in pipe.Connect()
                (fun b -> 
                    pipe.Write(Array.append (System.BitConverter.GetBytes(b.Length)) b,0,b.Length+4)
                    pipe.WaitForPipeDrain()),
                (fun () -> 
                    let b = Array.zeroCreate 4
                    pipe.Read(b,0,4)|>ignore
                    let b' = System.BitConverter.ToInt32(b,0)|>Array.zeroCreate
                    pipe.Read(b',0,b'.Length)|>ignore
                    b)
            //elif hasflags [|"-f";"--file"|] then 
                //System.IO.File.OpenRead(uri) :> System.IO.Stream
            //elif hasflags [|"-c";"--cyclic"|] then null,null//do Cyclic stuff
            else printfn "No valid stream type indicator";exit -2
        let hash = 
            if hasflags [|"-h";"--hash"|] then 
                match (getarg [|"-h";"--hash"|]).ToLower() with
                |"md5" -> MD5.Create() :> HashAlgorithm |> Some
                |"sha1" -> SHA1.Create() :> HashAlgorithm |> Some
                |"sha256" -> SHA256.Create() :> HashAlgorithm |> Some
                |"sha512" -> SHA512.Create() :> HashAlgorithm |> Some
                |"sha3-256" -> SHA3 256 :> HashAlgorithm |> Some
                |"sha3-512" -> SHA3 512 :> HashAlgorithm |> Some
                |s -> printfn "%A is not a valid hash algorithm" s;exit -4
            else warn "no verification"; None
        let verifier = 
            if hasflags [|"-r";"--rsa-vefify"|] then
                let rsa = new RSACryptoServiceProvider()
                rsa.ImportCspBlob(System.Convert.FromBase64String(getarg [|"-r";"--rsa-verify"|]))
                rsa.VerifyData |> Some
            else warn "no verification"; None
        let read = 
            let v =
                match hash with
                |Some(h) -> 
                    match verifier with
                    |Some(v) -> fun (b:byte[]) -> let split = h.HashSize/8 in let hash,b' = b.[..split-1],b.[split..] in v(b',h,hash)
                    |None -> fun (b:byte[]) -> let split = h.HashSize/8 in let hash,b' = b.[..split-1],b.[split..] in (h.ComputeHash b') = hash
                |None -> (fun _ -> true)
            fun () ->
                let m = ostream()
                if v m then Some(m) else None
        let write = 
            match hash with
            |Some(h) -> fun (b:byte[]) -> Array.append(h.ComputeHash b) b |> istream
            |None -> fun (b:byte[]) -> istream b
        do match hash with
            |Some(h) -> write (System.BitConverter.GetBytes(h.GetHashCode()))
            |None -> write [|0uy|]
        let writequeue = new System.Collections.Generic.Queue<string>()
        do (Process.OfExpr <@ while true do writequeue.Dequeue() |> printfn "%s" @>).Start()
        let processes = ResizeArray()
        member x.Processes = processes.AsReadOnly():>seq<_>
        member x.Load = processes.Count
        member x.DoProcessAsync() = 
            match read() with 
            |Some(r) -> 
                let p = Process.OfBytes(r)
                processes.Add(p)
                sprintf "Starting process %A from %A" p.Guid p.Parent |> writequeue.Enqueue
                p.Start()
                p.Wait()
                processes.Remove(p)|>ignore
                sprintf "Finished process %A from %A" p.Guid p.Parent |> writequeue.Enqueue
                (if p.IsControl then (p.Result.Value:?>RemoteControl).f x |> makeserializable else p.GetSerializableResult())|>getbytes|>compress|>istream
            |None -> warn "bad process"

    type RemoteSlave(send,recieve) = 
        member x.Control<'Return>(c) = c|>makeserializable|>getbytes|>compress|>send;recieve()|>decompress|>SerializableObject.OfBytes|>unbox<'Return>
        member x.Processes : seq<Process> = {f = fun (s:Slave) -> s.Processes|>box}|>x.Control
        member x.DoProcessAsync(p:Process) = p|>getbytes|>compress|>send;recieve()|>decompress|>SerializableObject.OfBytes
        member x.Load = x.Control<int>({f=fun (s:Slave) -> s.Load|>box})
    and Master(argv) = 
        let help () = ()
        let args = Array.append argv [|"--"|]|> Seq.pairwise |> Map.ofSeq
        let getarg s = match Array.tryPick(fun s' -> args.TryFind s') s with |Some(v) -> v |None -> printfn "Option %s requires a parameter" s.[0];exit -3
        let hasflags f = Array.exists(fun s' -> args.ContainsKey s') f
        do if hasflags [|"-h";"--help";"/?"|] then help();exit 0
        let warn (m:string) = printfn "WARNING: %s" (m.ToUpper())
        do if not (hasflags [|"-u";"--uri"|]) then printfn "Requires stream uri (-u or --uri)";exit -1
        let istream,ostream =
            let uri = getarg [|"-u";"--uri"|]
            if hasflags [|"-t";"--tcp"|] then
                let ip,port = if hasflags [|"-6";"--ipv6"|] then let p = uri.IndexOf(']') in uri.[1..p-1],uri.[p+2..] else let p = uri.Split(':') in p.[0],p.[1]
                let c = (new TcpClient(ip,int port))
                (fun b -> c.GetStream().Write(b,0,b.Length)),(fun()-> while not(c.GetStream().DataAvailable)do()done;let b=Array.zeroCreate c.Available in c.GetStream().Read(b,0,b.Length)|>ignore;b)
            elif hasflags [|"-p";"--pipe"|] then 
                let pipe = new System.IO.Pipes.NamedPipeClientStream(uri) in pipe.Connect()
                (fun b -> 
                    pipe.Write(Array.append (System.BitConverter.GetBytes(b.Length)) b,0,b.Length+4)
                    pipe.WaitForPipeDrain()),
                (fun () -> 
                    let b = Array.zeroCreate 4
                    pipe.Read(b,0,4)|>ignore
                    let b' = System.BitConverter.ToInt32(b,0)|>Array.zeroCreate
                    pipe.Read(b',0,b'.Length)|>ignore
                    b)
            else printfn "No valid stream type indicator";exit -2
        let hash = 
            if hasflags [|"-h";"--hash"|] then 
                match (getarg [|"-h";"--hash"|]).ToLower() with
                |"md5" -> MD5.Create() :> HashAlgorithm |> Some
                |"sha1" -> SHA1.Create() :> HashAlgorithm |> Some
                |"sha256" -> SHA256.Create() :> HashAlgorithm |> Some
                |"sha512" -> SHA512.Create() :> HashAlgorithm |> Some
                |"sha3-256" -> SHA3 256 :> HashAlgorithm |> Some
                |"sha3-512" -> SHA3 512 :> HashAlgorithm |> Some
                |s -> printfn "%A is not a valid hash algorithm" s;exit -4
            else warn "no verification"; None
        let verifier = 
            if hasflags [|"-r";"--rsa-vefify"|] then
                let rsa = new RSACryptoServiceProvider()
                rsa.ImportCspBlob(System.Convert.FromBase64String(getarg [|"-r";"--rsa-verify"|]))
                rsa.VerifyData |> Some
            else warn "no verification"; None
        let signer : ((byte[]*obj) -> byte[]) option = 
            if hasflags [|"-R";"--rsa-sign"|] then
                let rsa = new RSACryptoServiceProvider()
                rsa.ImportCspBlob(System.Convert.FromBase64String(getarg [|"-r";"--rsa-verify"|]))
                if rsa.PublicOnly then printfn "not private signer";exit -5
                Some(rsa.SignData)
            else warn "no signing"; None
        let slaves = System.Collections.Generic.Dictionary<System.Guid,(byte[]->unit)*(unit -> byte[])>()
        let read = 
            let v =
                match hash with
                |Some(h) -> 
                    match verifier with
                    |Some(v) -> fun (b:byte[]) -> let split = h.HashSize/8 in let hash,b' = b.[..split-1],b.[split..] in v(b',h,hash)
                    |None -> fun (b:byte[]) -> let split = h.HashSize/8 in let hash,b' = b.[..split-1],b.[split..] in (h.ComputeHash b') = hash
                |None -> (fun _ -> true)
            fun () ->
                let m = ostream()
                if v m then Some(m) else None
    (*
        let write = 
            match hash with
            |Some(h) -> match signer with |Some(s) -> (fun b -> Array.append (s(b,h)) b |> istream) |None -> fun (b:byte[]) -> Array.append(h.ComputeHash b) b |> istream
            |None -> fun (b:byte[]) -> istream b
    *)
        do match hash with
            |Some(h) -> istream (System.BitConverter.GetBytes(h.GetHashCode()))
            |None -> istream [|0uy|]
        let rng = System.Random()
        let processes = ResizeArray()
        let writequeue = new System.Collections.Generic.Queue<string>()
        do (Process.OfExpr <@ while true do writequeue.Dequeue() |> printfn "%s" @>).Start()
        let slaves = new ResizeArray<RemoteSlave>()
        let mutable loads = Seq.map (fun (s:RemoteSlave) -> async{return s,s.Load}) slaves |> Async.Parallel |> Async.RunSynchronously
        member x.Nodes = slaves
        member x.Load = processes.Count
        member x.GetLoads = loads
        member x.UpdateLoads() = Seq.map (fun (s:RemoteSlave) -> async{return s,s.Load}) slaves |> Async.Parallel |> Async.RunSynchronously
        member x.DistributeProcessesAsync() = 
            match read() with 
            |Some(r) -> 
                let p = Process.OfBytes(r)
                processes.Add(p)
                sprintf "Starting process %A" p.Guid |> writequeue.Enqueue
                let r = p.GetResult()
                processes.Remove(p)|>ignore
                sprintf "Finished process %A" p.Guid |> writequeue.Enqueue
                match r with
                | :? seq<Process> -> 
                    let rec choose() = x.GetLoads()|>Array.fold(fun acc elem -> if snd(elem) > acc then snd(elem) else acc) 0|>Array.head
                    choose()
            |None -> warn "bad process"
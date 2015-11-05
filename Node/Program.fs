// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open Cyclic3.Alpha
open System.Net
open System.Net.Sockets
open System.Security.Cryptography
let help() = ()
let p n s = printfn "%s" (String.init n (fun _ -> "\t") + s)
let p' n s = printf "%s" (String.init n (fun _ -> "\t") + s)
let w s = printfn "%s" s
[<EntryPoint>]
let main argv = 
    let args = Array.append argv [|"--"|]|> Seq.pairwise |> Map.ofSeq
    let getarg s = match Array.tryPick(fun s' -> args.TryFind s') s with |Some(v) -> v |None -> printfn "Option %s requires a parameter" s.[0];exit -3
    let hasflags f = Array.exists(fun s' -> args.ContainsKey s') f
    if hasflags [|"-h";"--help";"/?"|] then help();exit 0
    let warn (m:string) = printfn "WARNING: %s" (m.ToUpper())
    p 0 "Initialising"
    p' 1 "Checking node status: "
    match hasflags [|"-s";"--slave"|]with
    |true ->
        w "slave"
        if not (hasflags [|"-u";"--uri"|]) then printfn "Requires stream uri (-u or --uri)";exit -1
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
        match hash with
        |Some(h) -> write (System.BitConverter.GetBytes(h.GetHashCode()))
        |None -> write [|0uy|]
        while true do
            match read() with
            |Some(v) -> 
                let p = v|>decompress|>Process.OfBytes
                p.Start()
                p.GetSerializableResult().GetBytes()|>compress|>write
            |None -> warn "bad verification"
    |false ->
        w "master"
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
        let signer : ((byte[]*obj) -> byte[]) option = 
            if hasflags [|"-R";"--rsa-sign"|] then
                let rsa = new RSACryptoServiceProvider()
                rsa.ImportCspBlob(System.Convert.FromBase64String(getarg [|"-r";"--rsa-verify"|]))
                if rsa.PublicOnly then printfn "not private signer";exit -5
                Some(rsa.SignData)
            else warn "no signing"; None
        let slaves = System.Collections.Generic
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
            |Some(h) -> match signer with |Some(s) -> (fun b -> Array.append (s(b,h)) b |> istream) |None -> fun (b:byte[]) -> Array.append(h.ComputeHash b) b |> istream
            |None -> fun (b:byte[]) -> istream b
        match hash with
        |Some(h) -> write (System.BitConverter.GetBytes(h.GetHashCode()))
        |None -> write [|0uy|]
    0 // return an integer exit code

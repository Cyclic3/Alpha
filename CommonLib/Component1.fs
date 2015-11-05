namespace Cyclic3.Alpha
open Swensen.Unquote
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Compiler.Interactive.Shell
type Process (t:System.Threading.ParameterizedThreadStart) =
    let r : obj option ref = ref None
    let mutable thread = new System.Threading.Thread(t)
    new(b:System.Runtime.Serialization.SerializationInfo,_) = Process(b.GetValue("t",typeof<System.Threading.ParameterizedThreadStart>)|>unbox<System.Threading.ParameterizedThreadStart>)
    new(e:Expr) = Process(System.Threading.ParameterizedThreadStart(fun (r) -> let r' = r:?>obj option ref in r':=(Some (e.Eval()))))
    member x.Start() = thread.Start(r)
    member x.Stop() = thread.Abort()
    member x.Wait() = thread.Join()
    member x.Result = r.Value
    member x.GetResult() =
        x.Wait()    
        x.Stop()
        x.Result.Value
    member x.GetBytes() = let m = new System.IO.MemoryStream() in let b = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter() in b.Serialize(m,x);m.ToArray()
    member x.GetSerializableResult() = Process.OfExpr(<@x.GetResult()@>)
    interface System.Runtime.Serialization.ISerializable with member x.GetObjectData(info,_) = info.AddValue("t",t)
    static member OfBytes(b:byte[]) = let m = new System.IO.MemoryStream(b) in let b = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter() in b.Deserialize(m)|>unbox<Process>
    static member OfAsync(a:Async<'a>) = 
        Process(System.Threading.ParameterizedThreadStart(fun (r) -> let r' = r:?>obj option ref in r':=(Some(Async.RunSynchronously a|>box))))
    static member OfExpr(e:Expr) = 
        Process(System.Threading.ParameterizedThreadStart(fun (r) -> let r' = r:?>obj option ref in r':=(Some (e.Eval()|>box))))

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
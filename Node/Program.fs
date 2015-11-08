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
    p 0 "Initialising"
    p' 1 "Checking node status: "
    match Array.exists(function |"-s"|"--slave" -> true |_ -> false) argv with
    |true -> w "slave";let n = Node.Slave(argv) in while true do n.DoProcess() done
    |false ->w "master";let m = Node.Master(argv) in while true do m.() done

    0 // return an integer exit code

// Author: Codex R17 implementation agent, 29 October 2026.
// Private generation/export only; no product package imports or test execution.
package main
import("bytes";"compress/gzip";"crypto/sha256";"encoding/base64";"encoding/json";"fmt";"os";"os/exec";"path/filepath";"regexp")
const wrapper="/work/tnrun/operator/antlr4131-private-toolchain-20261026/antlr4.sh"
var outputs=[]string{"cypher_lexer.go","cypher_parser.go","cypherparser_listener.go","cypherparser_base_listener.go","CypherLexer.interp","CypherLexer.tokens","CypherParser.interp","CypherParser.tokens"}
var baseHashes=map[string]string{"CypherLexer.g4":"b787220038a40a079c4d7c19dbf46793708cb5b10fc468971617e9a71244ca38","CypherParser.g4":"794a96fab01ade0d0c99042181f671d59bf0f5c21e54d2b19233a74054986d71","cypher_lexer.go":"90c15fd9741fa182b6f05a7764a6e6efa32c10eea6968d1a2f7f49a9b6e755e4","cypher_parser.go":"9d7b8ac094ff9fab2aee13b1a199668b039af59ef220def89077516afff0096c","cypherparser_listener.go":"0b1d29aa674f09c8418178e22a90f1618bdb5f75715c4237f7034a512e73871f","cypherparser_base_listener.go":"bf83bf7a4164127c73fb16735e5588b338dd97859553d3ec3680c1b193c3e961","CypherLexer.interp":"0c1762c4451c569e417d2742575b6cee9454176e2116c4b3fd1e79df08f70b4f","CypherLexer.tokens":"584e1787cc0789a6bf459fb9d67c3e7bfda16345f40454765d8cc0d61f29cc29","CypherParser.interp":"b3414d0b7dd537dada1f7439cb809ee6e20ea7e5d8a3eaef90fe912e92784bc3","CypherParser.tokens":"584e1787cc0789a6bf459fb9d67c3e7bfda16345f40454765d8cc0d61f29cc29"}

func must(e error){if e!=nil{panic(e)}}
func run(dir,name string,args ...string){c:=exec.Command(name,args...);c.Dir=dir;c.Stdout,c.Stderr=os.Stdout,os.Stderr;must(c.Run())}
func read(path string)[]byte{b,e:=os.ReadFile(path);must(e);return b}
func emit(prefix string,v any){b,e:=json.Marshal(v);must(e);fmt.Printf("%s %s\n",prefix,b)}
func generate(root,assets,arm string)string{
 d,e:=os.MkdirTemp("","r17-resume-codegen-");must(e)
 for _,n:=range []string{"CypherLexer.g4","CypherParser.g4"}{from:=filepath.Join(root,"pkg/cypher/antlr",n);if arm=="candidate"{from=filepath.Join(assets,"candidate-"+n)};b:=read(from);must(os.WriteFile(filepath.Join(d,n),b,0600));emit("R17_CODEGEN_INPUT",map[string]any{"arm":arm,"file":n,"sha256":fmt.Sprintf("%x",sha256.Sum256(b))})}
 run(d,"sh",wrapper,"-Dlanguage=Go","-no-visitor","-package","antlr","CypherLexer.g4")
 run(d,"sh",wrapper,"-Dlanguage=Go","-no-visitor","-package","antlr","CypherParser.g4")
 p:=filepath.Join(d,"cypher_parser.go");b:=read(p);b=bytes.ReplaceAll(b,[]byte("\nerrorExit:"),[]byte("\n\tgoto errorExit\n\nerrorExit:"));b=regexp.MustCompile(`\n\tgoto errorExit // Trick[^\n]*`).ReplaceAll(b,nil);must(os.WriteFile(p,b,0600))
 args:=[]string{"run","cmd/gofmt","-w"};for _,n:=range outputs[:4]{args=append(args,filepath.Join(d,n))};run(root,"go",args...)
 return d
}
func main(){root,e:=os.Getwd();must(e);assets:=filepath.Join(root,"lab/r17codegen")
 for n,want:=range baseHashes{got:=fmt.Sprintf("%x",sha256.Sum256(read(filepath.Join(root,"pkg/cypher/antlr",n))));if got!=want{panic("pinned release input hash mismatch: "+n)}}
 run(root,"sh",wrapper,"--java-version")
 control:=generate(root,assets,"base");defer os.RemoveAll(control);matched:=true
 for _,n:=range outputs{want:=read(filepath.Join(root,"pkg/cypher/antlr",n));got:=read(filepath.Join(control,n));same:=bytes.Equal(want,got);matched=matched&&same;emit("R17_CODEGEN_IDENTITY",map[string]any{"file":n,"byteIdentical":same,"wantSHA256":fmt.Sprintf("%x",sha256.Sum256(want)),"gotSHA256":fmt.Sprintf("%x",sha256.Sum256(got))})}
 if !matched{panic("current release unchanged generation differs; candidate not generated")}
 candidate:=generate(root,assets,"candidate");defer os.RemoveAll(candidate)
 for _,n:=range outputs{b:=read(filepath.Join(candidate,n));var compressed bytes.Buffer;z:=gzip.NewWriter(&compressed);_,e=z.Write(b);must(e);must(z.Close());emit("R17_GENERATED_ARTIFACT",map[string]any{"file":n,"bytes":len(b),"sha256":fmt.Sprintf("%x",sha256.Sum256(b)),"gzipBase64":base64.StdEncoding.EncodeToString(compressed.Bytes())})}
 fmt.Println("R17_RESUME_CODEGEN_COMPLETE unchangedReleaseIdentity=true productExecution=false")
}

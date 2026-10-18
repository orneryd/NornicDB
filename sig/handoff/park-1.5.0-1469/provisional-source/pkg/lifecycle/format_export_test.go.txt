package lifecycle

import (
 "encoding/base64"
 "crypto/sha256"
 "encoding/json"
 "fmt"
 "go/format"
 "os"
 "path/filepath"
 "testing"
)
func TestExportFamilyFormat(t *testing.T) {
 for _,dir:=range []string{"lifecycle","nornicdb","embed","cypher","search","server","mcp","nornicgrpc","qdrantgrpc","bolt","graphql"}{
  err:=filepath.WalkDir(filepath.Join("..",dir),func(path string,d os.DirEntry,err error)error{
   if err!=nil{return err};if d.IsDir()||filepath.Ext(path)!=".go"{return nil}
   raw,err:=os.ReadFile(path);if err!=nil{return err};formatted,err:=format.Source(raw);if err!=nil{return fmt.Errorf("%s: %w",path,err)}
   if string(raw)!=string(formatted){line,_:=json.Marshal(map[string]string{"path":filepath.ToSlash(filepath.Join("pkg",path[len("../"):])),"sha":fmt.Sprintf("%x",sha256.Sum256(raw)),"data":base64.StdEncoding.EncodeToString(formatted)});fmt.Println("FAMILY_FORMAT "+string(line))};return nil
  });if err!=nil{t.Fatal(err)}
 }
}

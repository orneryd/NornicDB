package resultstream

import "bytes"

// Pending reports whether this response still owns undelivered pages.
func (d *Delivery) Pending() bool { return d.pending.Load() }

// ObserveWritten records qid references in successfully written response bytes.
// Signed qids use unescaped base64url text. A bounded suffix covers references
// split across writes without retaining the response body. This does not yet
// acknowledge delivery: the transport must complete its final flush first.
func (d *Delivery) ObserveWritten(data []byte) bool {
 if !d.pending.Load() { return false }
 d.mu.Lock()
 visible:=false
 for i:=range d.discards {
  cursor:=&d.discards[i]
  cursor.observeWritten(data)
  visible=visible||cursor.written
 }
 d.mu.Unlock()
 return visible
}

// DeliveredWritten acknowledges only references emitted in a completed response.
// Fail still retires any produced page omitted by projection/field selection.
func (d *Delivery) DeliveredWritten() {
 d.finishWhere(func(cursor deliveryCursor)bool{return cursor.written},false)
}
func(c *deliveryCursor) observeWritten(data []byte) {
 if c.written||len(data)==0 {return}
 token:=[]byte(c.qid)
 if bytes.Contains(data,token) {c.written=true;c.suffix=nil;return}
 limit:=len(token)-1
 if c.suffix==nil {c.suffix=make([]byte,0,2*limit)}
 previous:=len(c.suffix)
 if previous>0 {
  c.suffix=append(c.suffix,data[:min(len(data),limit)]...)
  if bytes.Contains(c.suffix,token) {c.written=true;c.suffix=nil;return}
  c.suffix=c.suffix[:previous]
 }
 if len(data)>=limit {c.suffix=append(c.suffix[:0],data[len(data)-limit:]...)} else {
  c.suffix=append(c.suffix,data...)
  if len(c.suffix)>limit {
   copy(c.suffix,c.suffix[len(c.suffix)-limit:])
   c.suffix=c.suffix[:limit]
  }
 }
}


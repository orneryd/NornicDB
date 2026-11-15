import {useEffect,useId,useRef,type ReactNode} from 'react';
import {ArrowLeft} from 'lucide-react';
/** A routed task keeps context visible without a modal or focus trap. */
export function RoutedTaskPanel({open,title,onBack,backLabel,children}:{open:boolean;title:string;onBack:()=>void;backLabel:string;children:ReactNode}){
 const id=useId(),heading=useRef<HTMLHeadingElement>(null);
 useEffect(()=>{if(open)heading.current?.focus();},[open,title]);
 if(!open)return null;
 return <section className="routed-task-panel" aria-labelledby={id}><button type="button" onClick={onBack} className="task-back"><ArrowLeft size={14}/>{backLabel}</button><h2 id={id} tabIndex={-1} ref={heading}>{title}</h2>{children}</section>;
}

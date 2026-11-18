import {createContext,useContext,useEffect,useState,type ReactNode} from 'react';
import {useLocation} from 'react-router-dom';
import {chatClient,type AssistantStatus} from '../../../Bifrost/client';

const AssistantAvailability = createContext<{status:AssistantStatus|null;error:string}>({status:null,error:''});

export function AssistantAvailabilityProvider({children}:{children:ReactNode}) {
 const login=useLocation().pathname.replace(/\/$/,'')==='/login';
 const [value,setValue]=useState<{status:AssistantStatus|null;error:string}>({status:null,error:''});
 useEffect(()=>{
  if(login){setValue({status:null,error:''});return;}
  let live=true,busy=false;
  const refresh=async()=>{
   if(busy)return;busy=true;
   try{const status=await chatClient.status();if(live)setValue({status,error:''});}
   catch(error){if(live)setValue({status:null,error:error instanceof Error?error.message:'Assistant unavailable'});}
   finally{busy=false;}
  };
  void refresh();const timer=setInterval(()=>void refresh(),15000);
  return()=>{live=false;clearInterval(timer);};
 },[login]);
 return <AssistantAvailability.Provider value={value}>{children}</AssistantAvailability.Provider>;
}

export function useAssistantAvailability(){
 const value=useContext(AssistantAvailability);
 return {...value,available:value.status?.heimdall?.enabled===true};
}

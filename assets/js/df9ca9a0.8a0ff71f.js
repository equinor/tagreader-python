"use strict";(self.webpackChunkdocumentation=self.webpackChunkdocumentation||[]).push([[1252],{2693:(e,s,n)=>{n.r(s),n.d(s,{assets:()=>c,contentTitle:()=>o,default:()=>h,frontMatter:()=>a,metadata:()=>r,toc:()=>l});const r=JSON.parse('{"id":"about/usage/data-source","title":"Data sources","description":"Tagreader supports connecting to PI and IP.21 servers using a Web API interfaces. When calling certain","source":"@site/docs/about/usage/data-source.md","sourceDirName":"about/usage","slug":"/about/usage/data-source","permalink":"/tagreader-python/docs/about/usage/data-source","draft":false,"unlisted":false,"editUrl":"https://github.com/equinor/tagreader-python/tree/main/documentation/docs/about/usage/data-source.md","tags":[],"version":"current","sidebarPosition":2,"frontMatter":{"sidebar_position":2},"sidebar":"about","previous":{"title":"Basic usage","permalink":"/tagreader-python/docs/about/usage/basic-usage"},"next":{"title":"Fetching metadata","permalink":"/tagreader-python/docs/about/usage/fetching-metadata"}}');var i=n(4848),t=n(8453);const a={sidebar_position:2},o="Data sources",c={},l=[{value:"Listing available data sources",id:"listing-available-data-sources",level:2}];function d(e){const s={a:"a",code:"code",em:"em",h1:"h1",h2:"h2",header:"header",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,t.R)(),...e.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(s.header,{children:(0,i.jsx)(s.h1,{id:"data-sources",children:"Data sources"})}),"\n",(0,i.jsxs)(s.p,{children:["Tagreader supports connecting to PI and IP.21 servers using a Web API interfaces. When calling certain\nmethods, the user will need to tell tagreader which system and which connection method to use. This input argument is\ncalled ",(0,i.jsx)(s.code,{children:"imstype"})," , and can be one of the following case-insensitive strings:"]}),"\n",(0,i.jsxs)(s.ul,{children:["\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"piwebapi"})," : For connecting to OSISoft PI Web API"]}),"\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"aspenone"})," : For connecting to AspenTech Process Data REST Web API"]}),"\n"]}),"\n",(0,i.jsx)(s.h2,{id:"listing-available-data-sources",children:"Listing available data sources"}),"\n",(0,i.jsxs)(s.p,{children:["The method ",(0,i.jsx)(s.code,{children:"tagreader.list_sources()"})," can query for available PI and IP.21 servers available. Input arguments:"]}),"\n",(0,i.jsxs)(s.ul,{children:["\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"imstype"})," : The name of the IMS type to query. Valid values: ",(0,i.jsx)(s.code,{children:"piwebapi"})," and ",(0,i.jsx)(s.code,{children:"aspenone"}),"."]}),"\n"]}),"\n",(0,i.jsxs)(s.p,{children:["The following input arguments are only relevant when calling ",(0,i.jsx)(s.code,{children:"list_sources()"})," with a Web API ",(0,i.jsx)(s.code,{children:"imstype"})," ( ",(0,i.jsx)(s.code,{children:"piwebapi"})," or\n",(0,i.jsx)(s.code,{children:"aspenone"})," ):"]}),"\n",(0,i.jsxs)(s.ul,{children:["\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"url"})," (optional): Path to server root, e.g. ",(0,i.jsxs)(s.em,{children:['"',(0,i.jsx)(s.a,{href:"https://aspenone/ProcessData/AtProcessDataREST.dll",children:"https://aspenone/ProcessData/AtProcessDataREST.dll"}),'"']})," or\n",(0,i.jsxs)(s.em,{children:['"',(0,i.jsx)(s.a,{href:"https://piwebapi/piwebapi",children:"https://piwebapi/piwebapi"}),'"']}),". ",(0,i.jsx)(s.strong,{children:"Default"}),": Path to Equinor server corresponding to selected ",(0,i.jsx)(s.code,{children:"imstype"})," if"]}),"\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"imstype"})," is ",(0,i.jsx)(s.code,{children:"piwebapi"})," or ",(0,i.jsx)(s.code,{children:"aspenone"})," ."]}),"\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"verifySSL"})," (optional): Whether to verify SSL certificate sent from server. ",(0,i.jsx)(s.strong,{children:"Default"}),": ",(0,i.jsx)(s.code,{children:"True"}),"."]}),"\n",(0,i.jsxs)(s.li,{children:[(0,i.jsx)(s.code,{children:"auth"})," (optional): Auth object to pass to the server for authentication. ",(0,i.jsx)(s.strong,{children:"Default"}),": Kerberos-based auth objects\nthat work with Equinor servers. If not connecting to an Equinor server, you may have to create your own auth."]}),"\n"]}),"\n",(0,i.jsxs)(s.p,{children:["When called with ",(0,i.jsx)(s.code,{children:"imstype"})," set to ",(0,i.jsx)(s.code,{children:"pi"})," , ",(0,i.jsx)(s.code,{children:"list_sources()"})," will search the registry at\n",(0,i.jsx)(s.em,{children:"HKEY_CURRENT_USER\\Software\\AspenTech\\ADSA\\Caches\\AspenADSA{username}"})," for available PI servers. Similarly,\nif called with ",(0,i.jsx)(s.code,{children:"imstype"})," set to ",(0,i.jsx)(s.code,{children:"ip21"})," , ",(0,i.jsx)(s.em,{children:"HKEY_LOCAL_MACHINE\\SOFTWARE\\Wow6432Node\\PISystem\\PI-SDK"})," will be searched\nfor available IP.21 servers. Servers found through the registry are normally servers to which the user is authorized,\nand does not necessarily include all available data sources in the organization."]}),"\n",(0,i.jsx)(s.p,{children:(0,i.jsx)(s.strong,{children:"Example:"})}),"\n",(0,i.jsx)(s.pre,{children:(0,i.jsx)(s.code,{className:"language-python",children:'from tagreader import list_sources\nlist_sources("ip21")\nlist_sources("piwebapi")\n'})}),"\n",(0,i.jsxs)(s.p,{children:["When called with ",(0,i.jsx)(s.code,{children:"imstype"})," set to ",(0,i.jsx)(s.code,{children:"piwebapi"})," or ",(0,i.jsx)(s.code,{children:"aspenone"})," , ",(0,i.jsx)(s.code,{children:"list_sources()"})," will connect to the web server URL and\nquery for the available list of data sources. This list is normally the complete set of data sources available on the\nserver, and does not indicate whether the user is authorized to query the source or not."]}),"\n",(0,i.jsxs)(s.p,{children:["When querying Equinor Web API for data sources, ",(0,i.jsx)(s.code,{children:"list_sources()"})," should require no input argument except\n",(0,i.jsx)(s.code,{children:'imstype="piwebapi"'})," or ",(0,i.jsx)(s.code,{children:'imstype="aspenone"'}),". For non-Equinor servers, ",(0,i.jsx)(s.code,{children:"url"})," will need to be specified, as may ",(0,i.jsx)(s.code,{children:"auth"}),"\nand ",(0,i.jsx)(s.code,{children:"verifySSL"})," ."]})]})}function h(e={}){const{wrapper:s}={...(0,t.R)(),...e.components};return s?(0,i.jsx)(s,{...e,children:(0,i.jsx)(d,{...e})}):d(e)}}}]);
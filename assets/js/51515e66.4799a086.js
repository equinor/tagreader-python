"use strict";(self.webpackChunkdocumentation=self.webpackChunkdocumentation||[]).push([[7505],{6271:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>d,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>r,toc:()=>l});const r=JSON.parse('{"id":"about/introduction","title":"Introduction","description":"Tagreader is a Python package for reading timeseries data from the OSIsoft PI and Aspen Infoplus.21","source":"@site/docs/about/introduction.md","sourceDirName":"about","slug":"/about/introduction","permalink":"/tagreader-python/docs/about/introduction","draft":false,"unlisted":false,"editUrl":"https://github.com/equinor/tagreader-python/tree/main/documentation/docs/about/introduction.md","tags":[],"version":"current","sidebarPosition":1,"frontMatter":{"sidebar_position":1},"sidebar":"about","next":{"title":"Usage","permalink":"/tagreader-python/docs/category/usage"}}');var s=t(4848),a=t(8453);const i={sidebar_position:1},o="Introduction",d={},l=[{value:"System requirements",id:"system-requirements",level:2},{value:"Installation",id:"installation",level:2},{value:"Usage",id:"usage",level:2},{value:"Usage example",id:"usage-example",level:3},{value:"Jupyter Notebook Quickstart",id:"jupyter-notebook-quickstart",level:3},{value:"Contribute",id:"contribute",level:2}];function c(e){const n={a:"a",code:"code",h1:"h1",h2:"h2",h3:"h3",header:"header",li:"li",p:"p",pre:"pre",ul:"ul",...(0,a.R)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(n.header,{children:(0,s.jsx)(n.h1,{id:"introduction",children:"Introduction"})}),"\n",(0,s.jsx)(n.p,{children:"Tagreader is a Python package for reading timeseries data from the OSIsoft PI and Aspen Infoplus.21\nInformation Manufacturing Systems (IMS) systems. It is intended to be easy to use, and present as similar interfaces\nas possible to the backend historians."}),"\n",(0,s.jsx)(n.h2,{id:"system-requirements",children:"System requirements"}),"\n",(0,s.jsx)(n.p,{children:"The only requirements are Python >= 3.8, with Windows, Linux or macOS."}),"\n",(0,s.jsx)(n.h2,{id:"installation",children:"Installation"}),"\n",(0,s.jsx)(n.p,{children:"You can install tagreader directly into your project from pypi by using pip\nor another package manager."}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:'language-shell"',children:"pip install tagreader\n"})}),"\n",(0,s.jsx)(n.p,{children:"The following are required and will be installed:"}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsx)(n.li,{children:"pandas"}),"\n",(0,s.jsx)(n.li,{children:"requests"}),"\n",(0,s.jsx)(n.li,{children:"requests-kerberos"}),"\n",(0,s.jsx)(n.li,{children:"certifi"}),"\n",(0,s.jsx)(n.li,{children:"diskcache"}),"\n"]}),"\n",(0,s.jsx)(n.h2,{id:"usage",children:"Usage"}),"\n",(0,s.jsxs)(n.p,{children:["Tagreader easy to use for both Equinor internal IMS services, and non-internal usage. For non-internal usage\nyou simply need to provide the corresponding IMS service URLs and IMSType. See ",(0,s.jsx)(n.a,{href:"/tagreader-python/docs/about/usage/data-source",children:"data source"})," for details."]}),"\n",(0,s.jsx)(n.h3,{id:"usage-example",children:"Usage example"}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-python",children:'import tagreader\nc = tagreader.IMSClient("mysource", "aspenone")\nprint(c.search("tag*"))\ndf = c.read_tags(["tag1", "tag2"], "18.06.2020 08:00:00", "18.06.2020 09:00:00", 60)\n'})}),"\n",(0,s.jsx)(n.h3,{id:"jupyter-notebook-quickstart",children:"Jupyter Notebook Quickstart"}),"\n",(0,s.jsx)(n.p,{children:"Jupyter Notebook examples can be found in /examples. In order to run these examples, you need to install the\noptional dependencies."}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-shell",children:"pip install tagreader[notebooks]\n"})}),"\n",(0,s.jsxs)(n.p,{children:["The quickstart Jupyter Notebook can be found ",(0,s.jsx)(n.a,{href:"https://github.com/equinor/tagreader-python/blob/main/examples/quickstart.ipynb",children:"here"})]}),"\n",(0,s.jsxs)(n.p,{children:["For more details, see the ",(0,s.jsx)(n.a,{href:"/docs/about/usage/basic-usage",children:"Usage section"}),"."]}),"\n",(0,s.jsx)(n.h2,{id:"contribute",children:"Contribute"}),"\n",(0,s.jsx)(n.p,{children:"As Tagreader is an open source project, all contributions are welcome. This includes code, bug reports, issues,\nfeature requests, and documentation. The preferred way of submitting a contribution is to either create an issue on\nGitHub or to fork the project and make a pull request."}),"\n",(0,s.jsxs)(n.p,{children:["For starting contributing, see the ",(0,s.jsx)(n.a,{href:"/tagreader-python/docs/contribute/how-to-start-contributing",children:"contribute section"})]})]})}function u(e={}){const{wrapper:n}={...(0,a.R)(),...e.components};return n?(0,s.jsx)(n,{...e,children:(0,s.jsx)(c,{...e})}):c(e)}}}]);
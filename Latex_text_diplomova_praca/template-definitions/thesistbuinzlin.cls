% ============================================================================ %
% Tento dokument není zpravidla třeba editovat, obsahuje import a nastavení 
% balíčků, nastavení dokumentu, stylů a další nutné záležitosti.
%
% Kódování: UTF-8 (žluťoučký kůň úpěl ďábelšké ódy)
% ============================================================================ %

\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{template-definitions/thesistbuinzlin}[2025/09/16 LaTeX class for theses at TBU in Zlin]
\LoadClass[a4paper,12pt,twoside]{article}

% BALÍČKY

%\RequirePackage[T1]{fontenc}   % definice vnitřního kódování
\RequirePackage[utf8]{inputenc} % slouží pro definici kódování (při problémech zkusit zaměnit utf8x za utf8)
\RequirePackage{sourcesanspro}
%\RequirePackage{times}         % pro použití fontu times
%\RequirePackage{textcomp}
\RequirePackage{color}          % umožňuje použití barev
\RequirePackage{graphicx}       % rozšíření práce s grafikou
\RequirePackage{fancyhdr}       % detailnější nastavení záhlaví a zápatí
\RequirePackage{listings}       % balíček pro výpis kódu
\RequirePackage{tocloft}        % umožňuje pohodlné nastavení vzhledu obsahu, seznamu tabulek či obrázků
\RequirePackage{textcase}       % změna VeLiKoStI PíSmA
\RequirePackage{setspace}       % balíček umožňující nastavit řádkování na 1, 1.5, 2
\RequirePackage[table,rgb]{xcolor} % umožňuje použití barev (tabulky, výpisy)
\definecolor{colortitle}{RGB}{245,120,0}
\definecolor{colorpagenum}{cmyk}{0,0,0,0.65}
\RequirePackage[labelsep=colon,tableposition=top,justification=raggedright,singlelinecheck=off,font={footnotesize,bf,sf}]{caption} % přenastavení titulků u figur a tabulek
%\RequirePackage{subcaption}    % umožňuje uvádět obrázky formou "Obr. X (a) Obr. X (b)"
\RequirePackage{titlesec}       % pro nastavení vzhledu nadpisů
%\RequirePackage[srcstyle=leftnumhang,linenumbersep={\ }]{examplep} % pokročilejší sazba programového kódu
\RequirePackage{ifthen}         % umožňuje využití if, then
\RequirePackage{etoolbox}       % tool-box for LaTeX programmers
%\RequirePackage{afterpage}     % spouštění příkazů po next page break
%\RequirePackage{layout}        % zobrazí nastavení tiskového zrcadla (příkaz \layout)
%\RequirePackage{verbatim}      % vysází text bez formátování, tak jak je zapsán v souboru
%\RequirePackage{indentfirst}   % definuje odsazení prvního řádku odstavce
%\RequirePackage{makeidx}       % vytvoří rejstřík
\RequirePackage[hyphens]{url}   % pro vysázení internetové adresy s vylepšeným řádkovým zlomem
\RequirePackage[pdftex,pdfa,hidelinks,breaklinks,hyperindex,breaklinks=true,colorlinks=false]{hyperref} % vytváří křížové odkazy
%\RequirePackage{multicol}      % vícesloupcová sazba
%\RequirePackage{flafter}       % zajistí, aby se plovoucí objekty objevovali až za jejich umístěním v textu
\RequirePackage{chngcntr}       % umožňuje změnu nastavení číslování obrázků, tabulek i rovnic
\RequirePackage{xmpincl}        % pro aplikaci standardu PDF/A
\RequirePackage{hyperxmp}       % pro aplikaci standardu PDF/A
\RequirePackage{pdfpages}       % pro vkládání .pdf souborů
\RequirePackage{amsmath}        % balíček pro pokročilejší matematiku
\RequirePackage{mathptmx}       % nahrazuje balíček times


% moje packages


% ---------------------------------------------------------------------------- %

% NASTAVENÍ TISKOVÉHO ZRCADLA, ZÁHLAVÍ A ZÁPATÍ
% A4: 210 mm × 297 mm

\newcommand{\valueTextHeight}{220mm}    % výška tiskového zrcadla
\newcommand{\valueTextWidth}{155mm}     % šířka tiskového zrcadla
\newcommand{\valueVOffset}{-12mm}       % vertikální posunutí tiskového zrcadla
\newcommand{\valueSideMargin}{9.6mm}    % levý okraj
\newcommand{\valueOddSideMargin}{10mm}  % twoside: okraj liché stránky
\newcommand{\valueEvenSideMargin}{-5mm} % twoside: okraj sudé stránky

\newcommand{\valueHeadHeight}{6mm}      % záhlaví, výška
\newcommand{\valueHeadSep}{10mm}        % záhlaví, mezera

\textheight=\valueTextHeight
\textwidth=\valueTextWidth
\voffset=\valueVOffset
%\voffset=-1in
%\topmargin=-29mm

\ifthenelse{\boolean{@twoside}}{\setlength{\oddsidemargin}{\valueOddSideMargin}\setlength{\evensidemargin}{\valueEvenSideMargin}}{\evensidemargin=\valueSideMargin\oddsidemargin=\valueSideMargin}

\headheight=\valueHeadHeight
\headsep=\valueHeadSep

% Nastavení zápatí
\footskip=24mm
\cfoot{}
% "Vypnoutí" poznámek na okrajích
\marginparpush=0mm
\marginparwidth=0mm
\marginparsep=0mm

\pagestyle{fancy}

% Nastavení obalujících okrajů okolo titulků figur a tabulek
\captionsetup[figure]{aboveskip=5pt, belowskip=-5pt}
\captionsetup[table]{aboveskip=5pt, belowskip=0pt}
\captionsetup[lstlisting]{position=bottom, aboveskip=15pt, belowskip=10pt}

% Nastavení písma u tabulek
\AtBeginEnvironment{tabular}{\small\sffamily}

% Neodsazování prvních řádků po tabulkách a obrázcích není žel funkční
%\AfterEndEnvironment{table}{\noindent\ignorespaces}
%\AfterEndEnvironment{figure}{\noindent\ignorespaces}

% ---------------------------------------------------------------------------- %

% Nastavení výpisů
\lstset{
    language=Python,              % nebo jazyk, který používáte
    basicstyle=\ttfamily\small,   % základní styl písma
    keywordstyle=\color{blue},    % klíčová slova
    stringstyle=\color{red},      % řetězce
    commentstyle=\color{green!50!black}, % komentáře
    identifierstyle=\color{black},% identifikátory
    showstringspaces=false,
    numbers=left,
    numberstyle=\scriptsize\color{gray},
    stepnumber=1,
    numbersep=3pt,
    backgroundcolor=\color{white},% změna pozadí
    tabsize=2,
    captionpos=b,                 % titulek pod výpisem
    xleftmargin=4mm,
    xrightmargin=4mm,
    frame=tb,
    framesep=10pt,
    framerule=1pt,
    breaklines=true,
    breakatwhitespace=false,
    aboveskip=14pt,
    belowskip=0pt,
    escapeinside={(*@}{@*)},
    inputencoding=utf8,
    extendedchars=true,
    literate={á}{{\'a}}1 {č}{{\v{c}}}1 {ď}{{\v{d}}}1 {é}{{\'e}}1 {ě}{{\v{e}}}1 {í}{{\'i}}1 {ň}{{\v{n}}}1 {ó}{{\'o}}1 {ř}{{\v{r}}}1 {š}{{\v{s}}}1 {ť}{{\v{t}}}1 {ú}{{\'u}}1 {ů}{{\r{u}}}1 {ý}{{\'y}}1 {ž}{{\v{z}}}1
}


% ============================================================================ %
% NASTAVENÍ PÍSMA, ODSTAVCE, ROVNIC, POZNÁMEK

\setlength{\parindent}{0pt}             % velikost odstavcové zarážky na nulu
\def\thefootnote{\arabic{footnote} }    % poznámka pod čarou
\setlength{\skip\footins}{14mm}
\renewcommand{\footnoterule}{%
  \kern -3pt                            % posune čáru mírně nahoru
  \hrule width \textwidth               % vykreslí čáru na celou šířku textu
  \kern 2.6pt                           % vytvoří mezeru pod čárou
}
%\onehalfspacing % nastavím řádkování tímto způsobem nebo:
%\renewcommand{\baselinestretch}{1.5}   %nebo:
\setstretch{1.08}
\setlength{\parskip}{0pt}               % vertikální mezera mezi odstavci
%\def\label#1{{\sf ! #1 ! }}            % možnost zobrazení všech \label{}


% ============================================================================ %
% NASTAVENÍ ČÍTAČŮ

\setcounter{tocdepth}{3} % do obsahu se ukládají pouze první dvě úrovně kapitol


% ============================================================================ %
% PDF/A STANDARD

% http://www.mathstat.dal.ca/~selinger/pdfa/
% https://blog.zhaw.ch/icclab/creating-pdfa-documents-for-long-term-archiving/
% https://tex.stackexchange.com/questions/95792/device-independent-color

% Prerequisites: pdflatex, hyperref, xmpincl, pdfTeX at least in version 1.40.15 (in Linux add repository ppa:jonathonf/texlive, update and upgrade texlive-full)

% Validators:
%   http://pdfa.k.utb.cz:7070/
%   https://www.pdf-online.com/osa/validate.aspx
%   https://avepdf.com/pdfa-validation
%   https://www.pdf2go.com/validate-pdfa

% \convertDate converts D:20080419103507+02'00' to 2008-04-19T10:35:07+02:00
\def\convertDate{\getYear}
{\catcode`\D=12
  \gdef\getYear D:#1#2#3#4{\edef\xYear{#1#2#3#4}\getMonth}
}
\def\getMonth#1#2{\edef\xMonth{#1#2}\getDay}
\def\getDay#1#2{\edef\xDay{#1#2}\getHour}
\def\getHour#1#2{\edef\xHour{#1#2}\getMin}
\def\getMin#1#2{\edef\xMin{#1#2}\getSec}
\def\getSec#1#2{\edef\xSec{#1#2}\getTZh}
\def\getTZh +#1#2{\edef\xTZh{#1#2}\getTZm}
\def\getTZm '#1#2'{
  \edef\xTZm{#1#2}
  \edef\convDate{\xYear-\xMonth-\xDay T\xHour:\xMin:\xSec+\xTZh:\xTZm}
}
\expandafter\convertDate\pdfcreationdate

\pdfminorversion 7

\immediate\pdfobj stream attr{/N 3} file{template-definitions/sRGBIEC1966-2.1.icm}
\pdfcatalog{
  /OutputIntents [ <<
  /Type /OutputIntent
  /S/GTS_PDFA1
  /DestOutputProfile \the\pdflastobj\space 0 R
  /OutputConditionIdentifier (sRGB IEC61966-2.1)
  /Info(sRGB IEC61966-2.1)
 >> ]
}

\providecommand{\xmpOrg}{Tomas Bata University in Zlín, Czech Republic}
\providecommand{\xmpProducer}{}
\providecommand{\xmpDoi}{}
\providecommand{\xmpJournalnumber}{}
\providecommand{\xmpVolume}{}
\providecommand{\xmpIssue}{}
\providecommand{\xmpCoverDisplayDate}{}
\providecommand{\xmpCoverDate}{}
\providecommand{\xmpJournaltitle}{}
\providecommand{\xmpFirstpage}{}
\providecommand{\xmpLastpage}{}
\providecommand{\xmpAuthoritativeDomain}{}
\providecommand{\xmpCreatorTool}{}%pdfTeX

\newcommand{\setuppdfa}{
  \ifczech
    \providecommand{\xmpTitle}{\nazevcz}
    \providecommand{\xmpAuthor}{\autor}
    \providecommand{\xmpKeywords}{\klicovaslovacz}
    \hypersetup{
      pdftitle={\nazevcz},
      pdfauthor={\autor},
      pdfsubject={\nazevcz},
      pdfkeywords={\klicovaslovacz},
      %pdfproducer={pdfTeX-1.40.20},
      pdflang=la,
      pdfapart=3,
      pdfaconformance=B,
      pdflang={cz},
      pdfmetalang={cz}
    }
  \else \ifenglish
    \providecommand{\xmpTitle}{\nazeven}
    \providecommand{\xmpAuthor}{\autor}
    \providecommand{\xmpKeywords}{\klicovaslovaen}
    \hypersetup{
      pdftitle={\nazeven},
      pdfauthor={\autor},
      pdfsubject={\nazeven},
      pdfkeywords={\klicovaslovaen},
      %pdfproducer={pdfTeX-1.40.20},
      pdflang=la,
      pdfapart=3,
      pdfaconformance=B,
      pdflang={en},
      pdfmetalang={en}
    }
  \fi \fi
  
  \makeatletter
  \includexmp{template-definitions/pdfa-1b}
  \makeatother
}

% Zapínání zvýrazňování odkazů
\newcommand{\usecolouredlinks}{
  \hypersetup{
    pdfborder={0 0 1}, % Šířka rámečku
    pdfborderstyle={/S/U/W 1}
  }
}


% ============================================================================ %
% ÚPRAVA STYLŮ

\titlespacing*{\section}{0pt}{0pt}{26pt}
\titlespacing*{\subsection}{0pt}{18pt}{0pt}
\titlespacing*{\subsubsection}{0pt}{18pt}{0pt}

\titleformat*{\section}{\Large\sffamily\bfseries} %\MakeUppercase
\titleformat*{\subsection}{\large\sffamily\bfseries}
\titleformat*{\subsubsection}{\normalsize\sffamily\bfseries}
\titleformat*{\paragraph}{\normalsize\sffamily\bfseries}

% Definice nového stylu \comment -- komentář k šabloně
\newcommand{\comment}[1]{\color{red}#1\color{black}}

% Nečíslovaný nadpis (bude vysázen v obsahu)
\def\sectionwithoutnumbering#1{\clearpage\phantomsection\addcontentsline{toc}{section}{#1}\section*{#1}}

% Styl pro tabulky v příloze
% \tabpri{titulek}{definice sloupců}{data tabulky}
\def\tabpri#1#2#3{
  \begin{table}[h]
  \begin{center}
  #1
  \end{center}
  \begin{center}
  \begin{tabular}{#2}
  #3
  \end{tabular}
  \end{center}
  \end{table}
}
  
% Styl pro tabulky z MS Excelu exportované do EPS
% \extab{titulek}{rozměr (0.0 - 1.0)}{soubor}
\def\extab#1#2#3{
  \begin{table}
  %\captionwidth{#2\linewidth}
  %\changecaptionwidth
  \captionsetup{width=#2\linewidth}
  \caption{#1}
  \begin{center}
  \includegraphics[width=#2\linewidth]{#3}
  \end{center}
  \end{table}
}

\def\insertObjectivesSectionTitle{
    \ifczech
        \sectionwithoutnumbering{Cíle a metody zpracování práce}
    \else
        \sectionwithoutnumbering{Objectives and Methods of Thesis Development}
    \fi
}

% Příkaz pro vysázení seznamu obrázků
\def\insertlistoffigures{
  \cleardoublepage
  \phantomsection
  \ifczech
    \addcontentsline{toc}{section}{Seznam obrázků}
  \else \ifenglish
    \addcontentsline{toc}{section}{List of Figures}
  \fi \fi
  \renewcommand{\cftfigaftersnum}{: }
  \listoffigures
  \clearpage
}

% Příkaz pro vysázení seznamu tabulek
\def\insertlistoftables{
  \clearpage
  \phantomsection
  \ifczech
    \addcontentsline{toc}{section}{Seznam tabulek}
  \else \ifenglish
    \addcontentsline{toc}{section}{List of Tables}
  \fi \fi
  \renewcommand{\cfttabaftersnum}{: }
  \listoftables
  \clearpage
}

\newcommand{\paragraphindentationstart}[0]{
  \ifenglish
    \setlength{\parskip}{5mm}  % English indentation of paragraphs
  \else \ifczech
    \setlength{\parindent}{8mm} % Czech indentation of paragraphs
  \fi \fi
}

\newcommand{\paragraphindentationstop}[0]{
  \ifenglish
    \setlength{\parskip}{0mm}  % English indentation of paragraphs
  \else \ifczech
    \setlength{\parindent}{0mm} % Czech indentation of paragraphs
  \fi \fi
}

% Příkaz pro vysázení zjednodušeného seznamu literatury
\newcommand{\seznamlit}[1]{
  \clearpage
  \phantomsection
  \ifczech
    \addcontentsline{toc}{section}{Seznam použité literatury}
  \else \ifenglish
    \addcontentsline{toc}{section}{References}
  \fi \fi
  \begin{thebibliography}{99}
  #1
  \end{thebibliography}
}

\newcommand{\insertlistofreferences}{
  % ČSN ISO 690:2022, \citationmode je makro definované v preambuli
  \bibliographystyle{\ifenglish template-definitions/plainnat-en-\citationmode\else template-definitions/plainnat-cz-\citationmode\fi}
  \newpage
  \cleardoublepage
  \phantomsection
  \addcontentsline{toc}{section}{\ifenglish References \else \ifczech Seznam použité literatury \fi \fi}
  \ifthenelse{\equal{\citationmode}{authoryear}}{\setlength{\bibhang}{8mm}\setlength{\bibsep}{0mm}}{\setlength{\bibhang}{0mm}\setlength{\bibsep}{1mm}}
  \raggedright
  
  \iffhs 
   \footnotesize
   \setlength{\bibsep}{3mm}
  \else
    \small
  \fi

  \bibliography{text/references}  % references.bib
}

% Příkaz pro přípravu seznamu použitých zkratek a symbolů
\newcommand{\inserttitlelistofabbreviations}{
  \ifczech
    \sectionwithoutnumbering{Seznam použitých symbolů a zkratek}
  \else \ifenglish
    \sectionwithoutnumbering{List of Abbreviations}
  \fi \fi
  \vspace{-1mm}
}

% Příkaz vysází obsah v daném místě
\def\insertcontents{
  \deaktivujZapati
  \cleardoublepage
  \thispagestyle{empty}
  \tableofcontents
  \cleardoublepage
  \pagestyle{fancy}
  \aktivujZapati

  % Od obsahu dále každá nová kapitola na nové stránce
  %\AddToHook{cmd/section/before}{\clearpage} ... nehodí se, nově je kapitola na stránce s novým oddílem
}

% Následující kód řeší to, aby každá nová kapitola (section) se nacházela na nové stránce, jen pokud jí nepředchází nový oddíl (part)
% Jednoduché počítadlo
\newcounter{sectioncounter}
% Před každou sekcí se zkontroluje hodnota počítadla
\pretocmd{\section}{%
  \ifnum\value{sectioncounter}<1\relax %
    \setcounter{sectioncounter}{1}%
  \else
    \clearpage
  \fi
}{}{}
% Po každé části (part) se resetuje počítadlo na 0
\pretocmd{\part}{\setcounter{sectioncounter}{0}}{}{}

% Zkrácení stylu \textbf na \b
\def\b#1{
  \textbf{#1}
}

% \bi = tučná kurzíva
\newcommand{\bi}[1]{\textbf{\textit{#1}}}

% \it = kurzíva
\renewcommand{\it}[1]{\textit{#1}}

% Nastaveni nezobrazování zahlavi dokumentu
\newcommand{\deaktivujZapati}{
  \lhead{}
  \rhead{}
  \lfoot{}
  \rfoot{}
  \renewcommand{\headrulewidth}{0pt}
}

% Příkaz pro vložení zadání práce
\def\insertassignment#1{
  \clearpage
  \thispagestyle{empty}
  \voffset=\valueVOffset
  \ifthenelse{\boolean{@twoside}}{\setlength{\oddsidemargin}{\valueOddSideMargin}\setlength{\evensidemargin}{\valueEvenSideMargin}}{\evensidemargin=\valueSideMargin\oddsidemargin=\valueSideMargin}
  \headsep=\valueHeadSep
  \headheight=\valueHeadHeight
  \setlength{\parskip}{3pt}
  \textheight=\valueTextHeight
  \textwidth=\valueTextWidth
  \includepdf[pages={1},offset=30 -12mm]{#1}
  \includepdf[pages={2},offset=5 -12mm]{#1}
  \cleardoublepage
}

% Nastavení zobrazování zápatí dokumentu
\newcommand{\aktivujZapati}{
  %\renewcommand{\headrulewidth}{1pt}
  %\fancyhead[LE,RO]{\bfseries\thepage}
  
  %\ifczech
  %  \fancyhead[RE,LO]{\bfseries{UTB ve Zlíně}}
  %\else \ifenglish
  %  \fancyhead[RE,LO]{\bfseries{TBU in Zlín}}
  %\fi \fi
  \fancyfoot[LO,RE]{}
  \fancyfoot[LE,RO]{\color{colorpagenum}\rule{12mm}{1.4pt}\\[-1mm]\makebox(12mm,0)[c]{\small\sffamily\thepage}}
}

% Vloží titulní stranu
\renewcommand{\maketitle}{%
  \counterwithin{lstlisting}{section}
  
  \pagestyle{empty}
  \voffset=0mm\evensidemargin=0pt\oddsidemargin=0mm\parindent=0pt\headsep=0pt\headheight=0pt\parskip=0pt\textheight=272mm\textwidth=155mm
  \renewcommand{\baselinestretch}{0}
  
  \setlength{\unitlength}{1mm}
  \begin{picture}(\textwidth,0)
    \put(20,-69){\parbox[t][60mm][c]{150mm}{\raggedleft\color{colortitle}\setstretch{1.05}\bfseries\fontsize{19}{24}\selectfont\textsf{\ifczech \nazevcz \else \nazeven \fi}}}
    
    \put(18,-133){\makebox(155,0){\includegraphics[width=155mm]{graphics/title_line.png}}}

    \put(20,-152){\makebox(150,0)[r]{\raggedleft\fontsize{17}{22}\selectfont\textsf{\autor}}}
    \put(20,-162){\makebox(150,0)[r]{\raggedleft\bfseries\fontsize{16}{21}\selectfont\textsf{\typeofwork\ | \rok}}}
    
    \put(18,-197){\makebox(155,0){\includegraphics[width=155mm]{graphics/title_line.png}}}
    
    \put(20,-209){\makebox(150,0)[r]{\raggedleft\bfseries\fontsize{15}{20}\selectfont\textsf{\ifczech Univerzita Tomáše Bati ve Zlíně\else Tomas Bata University in Zlín\fi}}}
    \put(20,-218){\makebox(150,0)[r]{\raggedleft\fontsize{13}{20}\selectfont\textsf{\faculty}}}

    \put(18,-232){\makebox(155,0){\includegraphics[width=155mm]{graphics/title_line.png}}}

    \put(125,-244){\makebox(45,0){\includegraphics[width=45mm]{\ifczech graphics/logo_utb_cz.png \else graphics/logo_utb_en.png \fi}}}
  \end{picture}
  
  \clearpage
  \newpage
  \null
  \clearpage
  \newpage
  
  \begin{picture}(\textwidth,0)
    \put(125,-244){\makebox(45,0){\includegraphics[width=45mm]{\ifczech graphics/logo_utb_cz.png \else graphics/logo_utb_en.png \fi}}}
  \end{picture}
}


% Vloží stranu s abstraktem a klíčovými slovy v češtině a angličtině
\newcommand{\insertabstractandkeywords}{
  \cleardoublepage
  \pagestyle{empty}
  {\Large\sffamily\bfseries{Abstrakt}} %\MakeTextUppercase{}
  
  \vspace{10mm}
  \paragraphindentationstart
  \noindent\abstraktcz
  \paragraphindentationstop
  
  \vspace{4mm}
  \textbf{Klíčová slova}
  \vskip-1mm
  \klicovaslovacz
  
  \vspace{23mm}
  {\Large\sffamily\bfseries{Abstract}}
  
  \vspace{10mm}
  \paragraphindentationstart
  \noindent\abstrakten
  \paragraphindentationstop
  
  \vspace{4mm}
  \textbf{Keywords}
  \vskip-1mm
  \klicovaslovaen
}


% ============================================================================ %
% NASTAVENÍ ZOBRAZENÍ PŘÍLOH -- SEZNAM, ČÍSLOVÁNÍ, VLASTNÍ STYL

\makeatletter % tímto příkazem dávám najevo, že budou následovat editace příkazů ze šablony

% Definice seznamu příloh
\def\insertlistofappendices{
  \newpage
  \phantomsection
  \setcounter{section}{0}
  \ifczech
    \addcontentsline{toc}{section}{Seznam příloh}
    \@restonecolfalse\if@twocolumn\@restonecoltrue\onecolumn\fi
    {\section*{Seznam příloh}}
  \else \ifenglish
    \addcontentsline{toc}{section}{List of Appendices}
    \@restonecolfalse\if@twocolumn\@restonecoltrue\onecolumn\fi
    {\section*{List of Appendices}}
  \fi \fi
  \@mkboth{List of Appendices}{List of Appendices}
  %\vspace{4mm}
  {\large\sffamily\bfseries{\ifczech Přílohy vložené v závěrečné práci\else Appendices included in the final thesis\fi}}
  \vspace{2mm}
  
  \@starttoc{loa}\if@restonecol\twocolumn\fi

  \setcounter{section}{0}
  \vspace{10mm}
  {\large\sffamily\bfseries{\ifczech Ostatní přílohy\else Others\fi}}
  \vspace{2mm}
  
  \@starttoc{lao}\if@restonecol\twocolumn\fi
}

\def\ext@appendix{loa}
\def\tocname{loa}
\newcounter{appendices}[section]

\def\ext@appendix{lao}
\def\tocname{lao}
\newcounter{appendicesother}[section]

% Příkazu pro vložení \addappendix{nazev_prilohy} pro vložení nové přílohy
\newcommand{\addappendix}[1]{
  \clearpage
  \thispagestyle{empty}
  \refstepcounter{appendices}
  %\voffset=-30mm % vertikální posun
  \addtocontents{loa}{\protect\makebox[24mm][l]{\ifczech Příloha\else \ifenglish Appendix\fi\fi\ \@Alph\c@appendices:}#1\par}
  %Varianta s číslováním stran: \addtocontents{loa}{\protect\makebox[24mm][l]{\ifczech Příloha\else \ifenglish Appendix\fi\fi\ \@Alph\c@appendices:}#1 \protect\cftdotfill{0.3}\ \thepage\par}
  \ifczech
    \section*{Příloha\ \@Alph\c@appendices: \Large\sffamily\bfseries{#1}}
  \else \ifenglish
    \section*{Appendix\ \@Alph\c@appendices: \Large\sffamily\bfseries{#1}}
  \fi \fi
  \par
}

% Příkaz pro zápis přílohy do seznamu ostatních příloh
\newcommand{\addappendixother}[3]{
  \refstepcounter{appendicesother}
  %\voffset=-30mm % vertikální posun
  \addtocontents{lao}{\protect\makebox[24mm][l]{#1}\protect\makebox[91mm][l]{#2}\protect\makebox[40mm][r]{#3}\par}
}

% ============================================================================ %
% NASTAVENÍ ZOBRAZENÍ SEZNAMU VÝPISŮ

\def\insertlistoflisting{
  \clearpage
  \phantomsection
  \ifczech
  \addcontentsline{toc}{section}{Seznam výpisů}
  \else \ifenglish
  \addcontentsline{toc}{section}{List of Listings}
  \fi \fi
  \begingroup
  \let\contentsname\lstlistlistingname
  \let\lst@temp\@starttoc \def\@starttoc##1{\lst@temp{lol}}
  \tableofcontents
  \endgroup
  \clearpage
}

% Přidání svislého prostoru na začátku seznamu výpisů
%\AtBeginDocument{\addtocontents{lol}{\protect\addvspace{100\p@}}}

%\AtBeginDocument{%
%  \let\c@lstlisting\relax
%  \newlistof[mylstlisting]{lstlisting}{lol}{\ifczech Seznam výpisů\else List of Listings\fi}
%  \renewcommand{\cftlstlistingfont}{\ifczech Výpis \else Listing \fi}
%  \renewcommand{\cftlstlistingaftersnum}{: }
%  \renewcommand{\cftlstlistingnumwidth}{3em}
%  \renewcommand{\cftlstlistingleader}{\cftdotfill{0.3}}
%}

\renewcommand\l@lstlisting[2]{\@dottedtocline{1}{0em}{3em}{\ifczech Výpis~#1 \else Listing~#1 \fi }{#2}}

% Nastavení vzhledu položek v seznamu výpisů
\def\@dotsep{0.3}
\def\@dottedtocline#1#2#3#4#5{
  \thispagestyle{fancy}
  \ifnum #1>\c@tocdepth \else
  \vskip \z@ \@plus.\p@
  {\hangindent #2\relax
    \hangafter \z@
    \rightskip \@tocrmarg \parfillskip -\rightskip
    \parindent #2\relax \@afterindenttrue
    \interlinepenalty\@M
    \leavevmode
    \@tempdima #3\relax
    \advance\leftskip \@tempdima \null\hskip -\leftskip
    {#4}\nobreak
    \leaders\hbox{$\m@th \mkern \@dotsep mu\hbox{.}\mkern \@dotsep mu$}\hfill
    \nobreak
    \hb@xt@\@pnumwidth{\hss #5}\par
  }
  \fi
}


% ============================================================================ %
% KONFIGURACE ODDÍLŮ (v obsahu i v textu)

\def\@part[#1]#2{
  \ifnum \c@secnumdepth >\m@ne
    \refstepcounter{part}
    \addcontentsline{toc}{section}{\protect\texorpdfstring{\makebox[8.5mm]{\Alph{part}\hfill} #1}{\Alph{part}\ #1}}
  \else
    \addcontentsline{toc}{section}{#1}
  \fi
  {\parindent \z@ \raggedright
  \interlinepenalty \@M
  \clearpage
  \normalfont
  \ifnum \c@secnumdepth >\m@ne
    \Large\bfseries
    \nobreak
  \fi
  \LARGE\sffamily\bfseries\Alph{part} \hspace{2mm} {#2}
  \markboth{}{}\par}
  \nobreak
  \vspace*{57mm}
  \@afterheading
}


% ============================================================================ %
% NASTAVENÍ FORMÁTU ČÍSLOVÁNÍ OBRÁZKŮ A TABULEK

\def\thefigure{\arabic{figure}}  % číslování obrázků typu (y)
\def\thetable{\arabic{table}}    % číslování tabulek typu (y)

% Oddělovač čísla figury a titulku je definován jako volitelný parametr u balíčku caption

% Nastavení číslování obrázků, tabulek i rovnic do formátu <číslo kapitoly>.<pořadové číslo>
\counterwithin{figure}{section}
\counterwithin{table}{section}
\counterwithin{equation}{section}
% Odsazeni titulku v seznamu obrázku a tabulek
\patchcmd{\@caption}{\csname the#1\endcsname}{\csname fnum@#1\endcsname}{}{}
%{\renewcommand*\numberline[1]{Fig. \,#1\space}}
%\renewcommand*\l@figure{\@dottedtocline{1}{0em}{5.0em}}
%\renewcommand*\l@table{\@dottedtocline{1}{0em}{5.0em}}

% Vynulování čítačů
\@addtoreset{table}{section}
\@addtoreset{figure}{section}
\@addtoreset{footnote}{section}
\@addtoreset{lstlisting}{section}

% Hack na vyřešení číslování seznamu použité literatury navzdory volbě u natbib author-year (novější verze natbib toto nezpůsobují)
%\renewcommand\@biblabel[1]{\hspace{-\labelsep}}

\makeatother % ukončení \makeatletter


% ============================================================================ %
% ÚPRAVA VZHLEDU OBSAHU, SEZNAMU OBRÁZKŮ A TABULEK

% OBSAH: NASTAVENÍ VELKÝCH PÍSMEN PRO NÁZVY SEKCÍ A HLAVNÍCH NADPISŮ

%\let\oldcontentsline\contentsline
%\def\contentsline#1#2{%
%  \expandafter\ifx\csname l@#1\endcsname\l@section
%    \expandafter\@firstoftwo
%  \else
%    \expandafter\@secondoftwo
%  \fi
%  {%
%    \oldcontentsline{#1}{\MakeTextUppercase{#2}}%
%  }{%
%    \oldcontentsline{#1}{#2}%
%  }%
%}

\setlength{\cftaftertoctitleskip}{28pt}
\setlength{\cftafterlottitleskip}{28pt}
\setlength{\cftafterloftitleskip}{28pt}

% Nastavení vertikální mezery před stylem část, nadpis 1--3
\setlength{\cftbeforepartskip}{4pt}
\setlength{\cftbeforesecskip}{4pt}
\setlength{\cftbeforesubsecskip}{4pt}
\setlength{\cftbeforesubsubsecskip}{4pt}

% Odsazení zleva pro styl část, nadpis 1--3
\setlength{\cftpartindent}{0mm}
\setlength{\cftsecindent}{0mm}
\setlength{\cftsubsecindent}{0mm}
\setlength{\cftsubsubsecindent}{0mm}

% Nastavení fontu pro styl část, nadpis 1--3
\renewcommand{\cftpartfont}{\bfseries}
\renewcommand{\cftsecfont}{\bfseries}
\renewcommand{\cftsubsecfont}{}% \scshape ... small caps
\renewcommand{\cftsubsubsecfont}{}

% Odsazení čísla a textu nadpisu/titulku, nadpis 1--3
\cftsetindents{sec}{0mm}{10mm}
\cftsetindents{subsec}{5mm}{12.5mm}
\cftsetindents{subsubsec}{10mm}{15mm}
\cftsetindents{fig}{0mm}{15mm}
\cftsetindents{tab}{0mm}{15mm}

% Nastavení vodící čáry pro styl část, nadpis 1--3, obrázky a tabulky
\renewcommand{\cftdot}{\ensuremath{.}} % tímto příkazem lze změnit vodící tečky v obsahu na jiný znak
\renewcommand{\cftpartleader}{\cftdotfill{0.3}}
\renewcommand{\cftsecleader}{\cftdotfill{0.3}}
\renewcommand{\cftsubsecleader}{\cftdotfill{0.3}}
\renewcommand{\cftsubsubsecleader}{\cftdotfill{0.3}}
\renewcommand{\cftfigleader}{\cftdotfill{0.3}}
\renewcommand{\cfttableader}{\cftdotfill{0.3}}

% Změna fontu pro text "Obsah", "Seznam obrázků" a "Seznam tabulek"
\renewcommand{\cfttoctitlefont}{\normalsize\bfseries\thispagestyle{empty}}
\renewcommand{\cftloftitlefont}{\normalsize\bfseries\thispagestyle{fancy}}
\renewcommand{\cftlottitlefont}{\normalsize\bfseries\thispagestyle{fancy}}

\renewcommand{\cfttabpresnum}{Tab. }
\setlength{\cftfignumwidth}{24mm}
\setlength{\cfttabnumwidth}{24mm}


% ============================================================================ %
% VSTUPY

% Nastavení a kontrola fakulty
\newcommand{\setfaculty}[1]{
  \newcommand{\facultyshort}{#1}
  
  \newif\iffai  \let\iffai\iffalse
  \newif\iffame \let\iffame\iffalse
  \newif\iffhs  \let\iffhs\iffalse
  \newif\ifflkr \let\ifflkr\iffalse
  \newif\iffmk  \let\iffmk\iffalse
  \newif\ifft   \let\ifft\iffalse
  \newif\ifuni  \let\ifuni\iffalse
  
  \ifthenelse{\equal{#1}{FAI}} {\let\iffai\iftrue}{}
  \ifthenelse{\equal{#1}{FAME}}{\let\iffame\iftrue}{}
  \ifthenelse{\equal{#1}{FHS}} {\let\iffhs\iftrue}{}
  \ifthenelse{\equal{#1}{FLKR}}{\let\ifflkr\iftrue}{}
  \ifthenelse{\equal{#1}{FMK}} {\let\iffmk\iftrue}{}
  \ifthenelse{\equal{#1}{FT}}  {\let\ifft\iftrue}{}
  \ifthenelse{\equal{#1}{UNI}} {\let\ifuni\iftrue}{}
  
  \ifczech
  \iffai \newcommand{\faculty}{Fakulta aplikované informatiky} 
    \else \iffame \newcommand{\faculty}{Fakulta managementu a ekonomiky} 
    \else \iffhs  \newcommand{\faculty}{Fakulta humanitních studií} 
    \else \ifflkr \newcommand{\faculty}{Fakulta logistiky a krizového řízení} 
    \else \iffmk  \newcommand{\faculty}{Fakulta multimediálních komunikací} 
    \else \ifft   \newcommand{\faculty}{Fakulta technologická} 
    \else \ifuni  \newcommand{\faculty}{Univerzitní institut} 
    \else \errmessage{Chyba nastaveni fakulty}
    \fi \fi \fi \fi \fi \fi \fi
  \else \ifenglish
  \iffai \newcommand{\faculty}{Faculty of Applied Informatics} 
    \else \iffame \newcommand{\faculty}{Faculty of Management and Economics} 
    \else \iffhs  \newcommand{\faculty}{Faculty of Humanities} 
    \else \ifflkr \newcommand{\faculty}{Faculty of Logistics and Crisis Management} 
    \else \iffmk  \newcommand{\faculty}{Faculty of Multimedia Communications} 
    \else \ifft   \newcommand{\faculty}{Faculty of Technology} 
    \else \ifuni  \newcommand{\faculty}{University Institute} 
    \fi \fi \fi \fi \fi \fi \fi
  \fi \fi
}

% Nastavení a kontrola typu práce
\newcommand{\setthesistype}[1]{
  \newif\ifbp \let\ifbp\iffalse
  \newif\ifdp \let\ifdp\iffalse
  
  \ifthenelse{\equal{#1}{BP}}{\let\ifbp\iftrue}{}
  \ifthenelse{\equal{#1}{DP}}{\let\ifdp\iftrue}{}
  
  \ifbp 
    \newcommand{\typeofwork}{\ifczech Bakalářská práce\else Bachelor's thesis\fi} 
  \else \ifdp 
    \newcommand{\typeofwork}{\ifczech Diplomová práce\else Master's thesis\fi} 
  \else
    \errmessage{Chyba nastaveni typu prace}
  \fi \fi
}

% Nastavení roku
\newcommand{\setthesisyear}[1]{
  \newcommand{\rok}{#1}
}

% Nastavení jména
\newcommand{\setauthor}[1]{
  \newcommand{\autor}{#1}
}

% Nastavení názvu
\newcommand{\settitlecz}[1]{
  \newcommand{\nazevcz}{#1}
}
\newcommand{\settitleen}[1]{
  \newcommand{\nazeven}{#1}
}

% Nastavení abstraktu
\newcommand{\setabstractcz}[1]{
  \newcommand{\abstraktcz}{#1}
}
\newcommand{\setabstracten}[1]{
  \newcommand{\abstrakten}{#1}
}

% Nastavení klíčových slov
\newcommand{\setkeywordscz}[1]{
  \newcommand{\klicovaslovacz}{#1}
}
\newcommand{\setkeywordsen}[1]{
  \newcommand{\klicovaslovaen}{#1}
}

% Nastavení a kontrola jazyka
\newcommand{\setthesislanguage}[1]{
  \newcommand{\jazyk}{#1}
  
  \newif\ifczech   \let\ifczech\iffalse
  \newif\ifenglish \let\ifenglish\iffalse
  
  \ifthenelse{\equal{#1}{CZ}}{\let\ifczech\iftrue}{}
  \ifthenelse{\equal{#1}{EN}}{\let\ifenglish\iftrue}{}
  
  \ifczech \else \ifenglish \else
    \errmessage{Chyba nastaveni jazyka}
  \fi \fi
  
  \ifczech
    \RequirePackage[czech]{babel}
    % Vlastní definice názvů
    \addto\captionsczech{\renewcommand{\contentsname}  {\Large\sffamily{Obsah}}} %\MakeUppercase
    \addto\captionsczech{\renewcommand{\refname}       {\Large\sffamily{Seznam použité literatury}}}
    \addto\captionsczech{\renewcommand{\listfigurename}{\Large\sffamily{Seznam obrázků}}}
    \addto\captionsczech{\renewcommand{\listtablename} {\Large\sffamily{Seznam tabulek}}}
    \addto\captionsczech{\renewcommand{\lstlistlistingname}{\Large\sffamily{Seznam výpisů}}}
    \addto\captionsczech{\renewcommand{\figurename}    {Obr.}}
    \addto\captionsczech{\renewcommand{\tablename}     {Tab.}}
    \renewcommand{\cftfigpresnum}{Obr. } %Lze pozměnit na Obrázek či Obr.
    \renewcommand{\cfttabpresnum}{Tab. }
    \renewcommand{\lstlistingname}{Výpis }
  \else \ifenglish
    \RequirePackage[english]{babel}
    % Vlastní definice názvu
    \addto\captionsenglish{\renewcommand{\contentsname}  {\Large\sffamily{Table of Contents}}}
    \addto\captionsenglish{\renewcommand{\refname}       {\Large\sffamily{References}}}
    \addto\captionsenglish{\renewcommand{\listfigurename}{\Large\sffamily{List of Figures}}}
    \addto\captionsenglish{\renewcommand{\listtablename} {\Large\sffamily{List of Tables}}}
    \addto\captionsenglish{\renewcommand{\lstlistlistingname}{\Large\sffamily{List of Listings}}}
    \addto\captionsenglish{\renewcommand{\figurename}    {Fig.}}
    \addto\captionsenglish{\renewcommand{\tablename}     {Tab.}}
    \renewcommand{\cftfigpresnum}{Fig. }
    \renewcommand{\cfttabpresnum}{Tab. }
    \renewcommand{\lstlistingname}{Lst. }
  \fi \fi
}

% Nastavení citačního módu
\newcommand{\setcitationmode}[1]{
  \ifthenelse{\equal{#1}{authoryear}}
    {\def\citationmode{authoryear}}
    {\ifthenelse{\equal{#1}{numbers}}
      {\def\citationmode{numbers}}
      {\errmessage{Chyba nastaveni jazyka}}
    }
  \RequirePackage[\citationmode]{natbib} % balíček pro sazbu použitých zdrojů
}

% Nastavení vertikálního odsazení nad rovnicemi/soustavami rovnic (první parametr) a pod (druhý parametr)
\newcommand{\setmarginaroundequations}[2]{
  \let\oldequation=\equation
  \let\endoldequation=\endequation
  \renewenvironment{equation}{\vspace{#1}\begin{oldequation}}{\end{oldequation}\vspace{#2}}
  
  \let\oldeqnarray=\eqnarray
  \let\endoldeqnarray=\endeqnarray
  \renewenvironment{eqnarray}{\vspace{#1}\begin{oldeqnarray}}{\end{oldeqnarray}\vspace{#2}}
}

% Nastavení vertikálního odsazen nad tabulkami (první parametr) a pod (druhý parametr)
\newcommand{\setmarginaroundtables}[2]{
  \let\oldtable=\table
  \let\endoldtable=\endtable
  \renewenvironment{table}{\vspace{#1}\begin{oldtable}}{\end{oldtable}\vspace{#2}}
}

% Nastavení vertikálního odsazen nad obrázky (první parametr) a pod (druhý parametr)
\newcommand{\setmarginaroundfigures}[2]{
  \let\oldfigure=\figure
  \let\endoldfigure=\endfigure
  \renewenvironment{figure}{\vspace{#1}\begin{oldfigure}}{\end{oldfigure}\vspace{#2}}
}


% ============================================================================ %
% STRANA S PROHLÁŠENÍM

\newcommand{\insertdeclaration}[1]{
  \clearpage
  \thispagestyle{empty}

  \ifthenelse{\equal{#1}{CZ}}
  {
  \section*{Prohlášení autora závěrečné kvalifikační práce}
  \vspace{2mm}
  \textbf{Beru na vědomí, že}
  \begin{itemize}
    \setlength{\parskip}{0pt}
    \setlength{\itemsep}{0pt}
    \setstretch{1.05}
    %proti dělení slova: \mbox{systému}
    \item odevzdáním závěrečné práce souhlasím se zpřístupněním své práce podle zákona č. 111/1998 Sb., v platném znění bez ohledu na výsledek obhajoby;
    \item závěrečná práce bude uložena v elektronické podobě v univerzitním informačním systému;
    \item jedno vyhotovení závěrečné práce v listinné podobě bude ponecháno Univerzitě Tomáše Bati ve Zlíně k uložení; 
    \item na moji závěrečnou práci se plně vztahuje zákon č. 121/2000 Sb. o právu autorském, o právech souvisejících s právem autorským a o změně některých zákonů (autorský zákon) ve znění pozdějších právních předpisů, zejm. § 35 odst. 3; 
    \item podle § 60 odst. 1 autorského zákona má Univerzita Tomáše Bati ve Zlíně právo na~uzavření licenční smlouvy o užití školního díla v rozsahu § 12 odst. 4 autorského zákona; 
    \item podle § 60 odst. 2 a 3 mohu užít své dílo – závěrečnou práci – nebo poskytnout licenci k jejímu využití jen s předchozím písemným souhlasem Univerzity Tomáše Bati ve Zlíně, která je oprávněna v takovém případě ode mne požadovat přiměřený příspěvek na úhradu nákladů, které byly Univerzitou Tomáše Bati ve Zlíně na vytvoření díla vynaloženy (až do jejich skutečné výše); 
    \item pokud bylo k vypracování závěrečné práce využito softwaru poskytnutého Univerzitou Tomáše Bati ve Zlíně nebo jinými subjekty pouze ke studijním a výzkumným účelům (tj. k nekomerčnímu využití), nelze výsledky závěrečné práce využít ke komerčním účelům; 
    \item pokud je výstupem závěrečné práce jakýkoliv softwarový produkt, považují se za \mbox{součást} práce rovněž i zdrojové kódy, popř. soubory, ze kterých se projekt skládá; neodevzdání této součásti může být důvodem k neobhájení práce.
  \end{itemize}
  
  \textbf{Prohlašuji, že}
  \begin{itemize}
    \setlength{\parskip}{0pt}
    \setlength{\itemsep}{0pt}
    \setstretch{1.05}
    \item jsem na závěrečné práci pracoval(a) samostatně a použitou literaturu jsem řádně citoval(a); v případě publikace výsledků budu uveden(a) jako spoluautor;
    \item odevzdaná verze závěrečné práce a verze elektronická nahraná do IS/STAG jsou obsahově totožné.
  \end{itemize}
  \bigskip
  \ifflkr
    V Uherském Hradišti, dne \dots\dots\dots\dots\dots\dots\dots
    \hspace{9mm}\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots
  \else
    Ve Zlíně, dne \dots\dots\dots\dots\dots\dots\dots
    \hspace{30mm}\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots
  \fi

  \vspace*{-2mm}\hspace{110mm}podpis autora
  
  }{% else english
  
  \section*{Final Thesis Author's Declaration}
  \textbf{I acknowledge that}
  \begin{itemize}
    \setlength{\parskip}{0pt}
    \setlength{\itemsep}{0pt}
    \setstretch{1.05}
    %\mbox{system}
    \item by submitting my final thesis, I consent to it being made available in accordance with the Act No. 111/1998 Coll., as amended, regardless of the outcome of the thesis defence; 
    \item the final thesis will be stored in electronic form in the university information system;
    \item one hard copy of the final thesis will be retained and archived by Tomas Bata University in Zlín; 
    \item my final thesis is fully subject to the Act No. 121/2000 Coll. on Copyright, on Rights Related to Copyright and on Amendment to Certain Acts (Copyright Act), as amended, particularly to Section 35 Paragraph 3; 
    \item pursuant to Section 60 Paragraph 1 of the Copyright Act, Tomas Bata University in Zlín has the right to conclude a licence agreement on the use of a school work as is specified in Section 12 Paragraph 4 of the Copyright Act; 
    \item pursuant to Section 60 Paragraphs 2 and 3, I may use my school work – the final thesis – or grant a licence for its use only with the prior written consent of Tomas Bata University in Zlín, which, in such a case, is entitled to require me to pay a reasonable contribution in order to cover the costs incurred by Tomas Bata University in Zlín in connection with the preparation of the work (up to their full amount); 
    \item if software provided by Tomas Bata University in Zlín or other entities was used for the preparation of the final thesis only for learning or research purposes (i.e. for non-commercial use), the conclusions of the final thesis cannot be used for commercial purposes; 
    \item if the output of the final thesis includes any type of software product, the source codes and/or files of which the project is comprised are also considered part of the thesis; failure to submit this part may result in failure to defend the thesis.
  \end{itemize}
  
  \textbf{I hereby declare that}
  \begin{itemize}
    \setlength{\parskip}{0pt}
    \setlength{\itemsep}{0pt}
    \setstretch{1.05}
    \item I have worked on my final thesis independently and have properly cited the literature used; in the event of publication of its conclusions, I shall be listed as a co-author;
    \item the submitted hard copy of the final thesis and the electronic version uploaded to the IS/STAG are identical in content.
  \end{itemize}
  \bigskip
  \ifflkr
    In Uherské Hradiště on \dots\dots\dots\dots\dots\dots\dots
    \hspace{14mm}\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots
  \else
    In Zlín on \dots\dots\dots\dots\dots\dots\dots
    \hspace{36mm}\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots\dots
  \fi

  \vspace*{-2mm}\hspace{106mm}author's signature
  }
  
  \cleardoublepage
}

% Tento příkaz vloží prázdnou stránku
\newcommand\insertblankpage{
  \clearpage
  \null
  \thispagestyle{empty}
  %\addtocounter{page}{-1} % Neinkrementace počtu stran (dělá nepořádek, pokud máme twopage - pak jsou najednou za sebou dvě liché či sudé strany)
  \newpage
}


% ============================================================================ %

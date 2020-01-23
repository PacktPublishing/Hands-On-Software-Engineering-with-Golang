package frontend

import (
	"html/template"
)

var (
	indexPageTemplate = template.Must(template.New("index").Parse(`
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Links 'R' Us</title>
    <style>
      .l{font-size:3em;font-weight:bold;text-align:center;text-shadow: 1px 1px 1px rgba(0,0,0,0.4);}
      .r{color:red;}
      .g{color:green;}
      .b{color:blue;}
      .o{color:orange;}
      .tc{margin-top:20px;text-align:center;}
      .t{border:1px solid lightgray;border-radius:24px;padding:10px;width:40%;}
      .sb{padding:10px;margin-top:20px;}
      input:focus{outline: none;}
			a{color:blue;text-decoration:none;font-size:0.8em;}
			a:visited{color:blue;}
  </style>
  </head>
  <body>
    <header class="l">
      <span class="b">L</span> <span class="r">i</span>
      <span class="o">n</span> <span class="b">k</span>
      <span class="r">s</span> <span class="g"> 'R' </span>
      <span class="o">U</span> <span class="r">s</span>
    </header>
    <section class="tc">
      <form action="{{.searchEndpoint}}">
      <input class="t" type="text" name="q" placeholder="Enter search term"/>
      <br>
      <input class="sb" type="submit" value="Search"/>
      </form>
			<br/><br/>
      <a rel="nofollow" href="{{.submitLinkEndpoint}}">Submit Web Site</a>
    </section>
  </body>
</html>
`))

	msgPageTemplate = template.Must(template.New("message").Parse(`
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Links 'R' Us | {{.messageTitle}} </title>
    <style>
      .is{display:inline;}
      .l{font-size:2em;font-weight:bold;text-shadow: 1px 1px 1px rgba(0,0,0,0.4);}
			.l a{text-decoration: none;}
      .r{color:red;}
      .g{color:green;}
      .b{color:blue;}
      .o{color:orange;}
      .tc{margin-top:20px;text-align:center;}
      .t{border:1px solid lightgray;border-radius:24px;padding:10px;width:40%;}
      .sb{padding:10px;margin-top:20px;}
			form{display:inline;padding-left:10px;}
      hr{border:1px solid gray;}
      .rc{padding:10px 20px;}
      .rc .rt {color:grey;font-size:1.1em;}
      input:focus{outline: none;}
    </style>
  </head>
  <body>
    <header>
      <section class="l is">
			  <a href="{{.indexEndpoint}}">
        <span class="b">L</span> <span class="r">i</span>
        <span class="o">n</span> <span class="b">k</span>
        <span class="r">s</span> <span class="g"> 'R' </span>
        <span class="o">U</span> <span class="r">s</span>
				</a>
      </section>
      <section class="is">
      <form action="{{.searchEndpoint}}">
        <input class="t" type="text" name="q" placeholder="Enter search term" value="{{.searchTerms}}"/>
        <input class="sb" type="submit" value="Search"/>
      </form>
      </section>
    </header>
    <hr/>
    <section class="rc">
      <span class="rt">{{.messageContent}}</span>
    </section>
  </body>
</html>
`))

	resultsPageTemplate = template.Must(template.New("results").Parse(`
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Links 'R' Us | Search</title>
    <style>
      .is{display:inline;}
      .l{font-size:2em;font-weight:bold;text-shadow: 1px 1px 1px rgba(0,0,0,0.4);}
			.l a{text-decoration: none;}
      .r{color:red;}
      .g{color:green;}
      .b{color:blue;}
      .o{color:orange;}
      .tc{margin-top:20px;text-align:center;}
      .t{border:1px solid lightgray;border-radius:24px;padding:10px;width:40%;}
      .sb{padding:10px;margin-top:20px;}
			form{display:inline;padding-left:10px;}
      hr{border:1px solid gray;}
      .rc{padding:10px 20px;}
      .rc .rt {color:grey;font-size:0.9em;}
			.rc .ml {text-decoration:none;display:inline-block;font-size:1.0em;font-weight:bold;margin-bottom:0;text-overflow:ellipsis;white-space:nowrap;overflow:hidden;}
			.rc cite{color:green;font-size:0.8em;display:block;margin-bottom:2px;}
			.rc .ms {text-align:justify;font-size:0.9em;}
			.rc .ms em{background-color:yellow;font-weight:bold;}
			.nb{padding:15px 20px;border-top:1px solid gray;}
			.nb a{padding-right:15px;text-decoration:none;color:blue;}
			.nb a:visited{color:blue;}
      input:focus{outline: none;}
    </style>
  </head>
  <body>
    <header>
      <section class="l is">
			  <a rel="nofollow" href="{{.indexEndpoint}}">
        <span class="b">L</span> <span class="r">i</span>
        <span class="o">n</span> <span class="b">k</span>
        <span class="r">s</span> <span class="g"> 'R' </span>
        <span class="o">U</span> <span class="r">s</span>
				</a>
      </section>
      <section class="is">
      <form action="{{.searchEndpoint}}">
        <input class="t" type="text" name="q" value="{{.searchTerms}}"/>
        <input class="sb" type="submit" value="Search"/>
      </form>
      </section>
    </header>
    <hr/>
		{{if .results}}
    <section class="rc">
      <span class="rt">Displaying results {{.pagination.From}} to {{.pagination.To}} from {{.pagination.Total}}.</span>
    </section>
		{{range .results}}
    <section class="rc">
      <a class="ml" rel="nofollow" href="{{.URL}}">{{.Title}}</a>
			<cite>{{.URL}}</cite>
      <section class="ms">{{.HighlightedSummary}}</section>
    </section>
		{{end}}
    <section class="nb">
		  {{if .pagination.PrevLink}}<a rel="nofollow" href="{{.pagination.PrevLink}}">Previous</a>{{end}}
		  {{if .pagination.NextLink}}<a rel="nofollow" href="{{.pagination.NextLink}}">Next</a>{{end}}
    </section>
		{{else}}
    <section class="rc">
      <span class="rt">Your search query did not match any pages.</span>
    </section>
		{{end}}
  </body>
</html>
`))

	submitLinkPageTemplate = template.Must(template.New("submit_link").Parse(`
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Links 'R' Us | Submit site</title>
    <style>
      .l{font-size:3em;font-weight:bold;text-align:center;text-shadow: 1px 1px 1px rgba(0,0,0,0.4);}
      .l a{text-decoration: none;}
      .r{color:red;}
      .g{color:green;}
      .b{color:blue;}
      .o{color:orange;}
      .tc{margin-top:20px;text-align:center;}
      tc fieldset{padding:10px 30px;}
      .sb{padding:5px 10px;margin-top:20px;}            
      .t{border:1px solid lightgray;padding:10px;width:90%}
			form{display:inline-block;width:400px;}
      input:focus{outline: none;}
      .msg {background-color:lightyellow;padding:10px 0;}
  </style>
  </head>
  <body>
    <header class="l">
     <a href="{{.indexEndpoint}}">
        <span class="b">L</span> <span class="r">i</span>
        <span class="o">n</span> <span class="b">k</span>
        <span class="r">s</span> <span class="g"> 'R' </span>
        <span class="o">U</span> <span class="r">s</span>
	</a>
    </header>
		{{if .messageContent}}<section class="tc msg">{{.messageContent}}</section>{{end}}
    <section class="tc">
      <form action="{{.submitLinkEndpoint}}" method="POST">
        <fieldset>
        <legend>Submit a web site to Links 'R' Us</legend>
        <input class="t" type="text" required="true" name="link" placeholder="https://"/>
				<br/>
        <input class="sb" type="submit" value="Submit"/>
        </fieldset>
      </form>
    </section>
  </body>
</html>
`))
)

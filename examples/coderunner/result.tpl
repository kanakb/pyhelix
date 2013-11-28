<!DOCTYPE html>
<html>
    <head>
        <title>Results</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">

        <!-- Bootstrap -->
        <link rel="stylesheet" href="static/bootstrap.min.css">
        <link rel="stylesheet" href="static/bootstrap-theme.min.css">
        <link rel="stylesheet" href="static/highlight-default.css">
    </head>
    <body>
        <div class="container">
            <div class="page-header">
                <h1>Execution Results</h1>
            </div>
            % for label, result in results:
            <h4>{{label}}</h4>
            <pre>{{result}}</pre>
            % end
            <div class="page-header">
                <h1>Code</h1>
            </div>
            <pre><code>{{prog}}</code></pre>
        </div>
        <script src="static/highlight.pack.js"></script>
        <script>hljs.initHighlightingOnLoad();</script>
    </body>
</html>

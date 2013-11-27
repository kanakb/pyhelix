<!DOCTYPE html>
<html>
    <head>
        <title>Run a Program</title>
        <link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.0.2/css/bootstrap.min.css">
    </head>
    <body>
        <h1>Submit a Program</h1>
        <form action="http://{{host}}:{{port}}/run" method="post">
            Set Destination:
            <select name="participant">
                <!--option value="star">* (all)</option-->
                <!--option value="random">random</option-->
                % for participant in participants:
                <option value="{{participant}}">{{participant}}</option>
                % end
            </select> <br />
            <textarea rows="20" cols="100" name="prog"></textarea> <br />
            <input type="submit" value="Submit" />
        </form>
    </body>
</html>

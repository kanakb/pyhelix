<!DOCTYPE html>
<html>
    <head>
        <title>Submit a Program</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">

        <!-- Bootstrap -->
        <link rel="stylesheet" href="static/bootstrap.min.css">
        <link rel="stylesheet" href="static/bootstrap-theme.min.css">
    </head>
    <body>
        <div class="container">
            <div class="page-header">
                <h1>Submit a Program</h1>
            </div>
            <form action="/run" method="post" role="form">
                <div class="form-group">
                    <label for="participant">Destination</label>
                    <select name="participant" class="form-control">
                        <option value="star">* (all)</option>
                        <option value="random">random</option>
                        % for participant in participants:
                        <option value="{{participant}}">{{participant}}</option>
                        % end
                    </select>
                </div>
                <div class="form-group">
                    <label for="prog">Code</label>
                    <textarea class="form-control" style="font-family:monospace;" rows="20" name="prog" autocapitalize="off" autocorrect="off"></textarea>
                </div>
                <button type="submit" value="Submit" class="btn btn-primary btn-lg">
                    Submit
                </button>
            </form>
        </div>
        <br /><br />
    </body>
</html>

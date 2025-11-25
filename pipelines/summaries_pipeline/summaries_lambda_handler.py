import boto3
import json
from datetime import datetime


def get_summary_data() -> str:
    """Reads the latest AI-generated summary from S3 and returns it as a string.

    Returns:
        str: The JSON summary as a string
    """

    s3_client = boto3.client('s3')

    BUCKET = "c20-power-monitor-s3"
    KEY = "summaries/summary-latest.json"

    response = s3_client.get_object(Bucket=BUCKET, Key=KEY)
    summary_str = response['Body'].read().decode('utf-8')

    summary_dict = json.loads(summary_str)
    summary = summary_dict.get('summary', "No summary available.")

    return summary


def generate_html_email(summary: str) -> str:
    """Generates an HTML email report from the summary data.

    Args:
        summary_dict: The summary dictionary

    Returns:
        str: Formatted HTML email report
    """

    timestamp = datetime.now().strftime("%Y-%m-%d")

    html_body = f"""\
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
            margin: 0;
            padding: 0;
        }}
        .container {{
            max-width: 600px;
            margin: 20px auto;
            background-color: #ffffff;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #00BCD4 0%, #CDDC39 100%);
            color: white;
            padding: 20px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 24px;
        }}
        .content {{
            padding: 20px;
        }}
        .timestamp {{
            background-color: #f0f9ff;
            border-left: 4px solid #00BCD4;
            color: #555;
            font-size: 12px;
            padding: 10px 12px;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        .timestamp-label {{
            font-weight: bold;
            color: #00BCD4;
        }}
        .summary {{
            line-height: 1.8;
            color: #333;
            font-size: 14px;
        }}
        .footer {{
            background-color: #f9f9f9;
            padding: 15px 20px;
            text-align: center;
            font-size: 12px;
            color: #999;
            border-top: 1px solid #eee;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>UK Power Monitor Daily Report</h1>
        </div>
        <div class="content">
            <div class="timestamp">
                <span class="timestamp-label">Report Generated:</span><br>{timestamp}
            </div>
            <div class="summary">{summary}</div>
        </div>
        <div class="footer">
            Power Monitor System
        </div>
    </div>
</body>
</html>
    """

    return html_body


def lambda_handler(event, context):
    """AWS Lambda handler to fetch summary data from S3.

    Args:
        event: Lambda event object
        context: Lambda context object

    Returns:
        dict: Response containing status code and summary data
    """
    summary_data = get_summary_data()

    html_email = generate_html_email(summary_data)

    return {
        'statusCode': 200,
        'body': html_email,
    }


if __name__ == "__main__":

    response = lambda_handler({}, {})
    email = response['body']

    # Save email locally for testing

    with open("summary_email.html", "w") as f:
        f.write(email)

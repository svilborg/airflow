import re
import json
import nltk
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist

def extract_commits():
    api = 'https://gitlabrepo.com/api/v4/'
    project_id = 100
    url = f'{api}projects/{project_id}/repository/commits'
    token = ''

    today = datetime.now().strftime('%Y-%m-%d')
    params = {
        'all': 'true',
        'since': today + 'T00:00:00Z',
        'until': today + 'T23:59:59Z',
        'per_page': 100
    }

    response = requests.get(url, params=params, headers={"PRIVATE-TOKEN": token})
    response.raise_for_status()

    commits = response.json()

    titles = [commit['title'] for commit in commits if 'Merge branch' not in commit['title']]
    text = ' '.join(titles)

    return text


def print_sentiment(**context):
    # Access the output of the previous task using context['ti']
    text = context['ti'].xcom_pull(task_ids='ExtractCommits')

    nltk.download('vader_lexicon')

    sid = SentimentIntensityAnalyzer()
    scores = sid.polarity_scores(text)

    if scores['compound'] >= 0.05:
        sentiment = "Positive"
    elif scores['compound'] <= -0.05:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    scores['sentiment'] = sentiment

    return scores


def get_freq(**context):
    # Access the output of the previous task using context['ti']
    text = context['ti'].xcom_pull(task_ids='ExtractCommits')

    nltk.download('punkt')
    nltk.download('stopwords')

    # Tokenize the commit messages
    tokens = word_tokenize(text)

    # Remove punctuation and convert to lowercase
    tokens = [token.lower() for token in tokens if re.match(r'\w', token)]

    # Define words to exclude
    excluded_words = ['hyperion-', 'sup-', 'redshift-']

    # Filter out stopwords
    stopwords_list = stopwords.words('english')

    tokens = [token for token in tokens if token
              not in stopwords_list
              and
              not any(
                  token.lower().startswith(word.lower()) for word in excluded_words
              )]

    # Calculate the frequency distribution of the tokens
    freq_dist = FreqDist(tokens)

    # Get the most common words and their frequencies
    top_words = freq_dist.most_common(5)

    return top_words


def notify_slack(**context):
    # Access the output of the previous task using context['ti']
    scores = context['ti'].xcom_pull(task_ids='CalcSentiment')
    top_words = context['ti'].xcom_pull(task_ids='CalcFreq')

    # Prepare the Slack message
    msg = slack_message_for_score(scores)
    msg = msg + slack_message_for_freq(top_words)

    webhook_url = 'https://hooks.slack.com/services/HOOOK_PARAMS'

    slack_data = {
        'username': 'webhookbot',
        'link_names': 1,
        'text': msg,
        'channel': '@myself'
    }

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )

    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )


def slack_message_for_freq(top_words):
    msg = f"""

:sparkles: Top Words in Commits :sparkles:
     
"""
    for word, count in top_words:
        msg += f"- {word}, #{count}\n"

    return msg


def slack_message_for_score(scores):
    # Determine sentiment and corresponding emoji
    if scores['compound'] >= 0.05:
        sentiment = "Positive :grinning:"
    elif scores['compound'] <= -0.05:
        sentiment = "Negative :disappointed:"
    else:
        sentiment = "Neutral :neutral_face:"

    msg = f"""
            :checkered_flag: Today's mood is {sentiment}

    *+*: {scores['pos']}
    *-*: {scores['neg']}
    *=*: {scores['neu']}
    *T*: {scores['compound']}
       
"""

    log(msg)

    return msg


def log(msg):
    print('-------------------------------------------------------------------------------------')
    print(msg)
    print('-------------------------------------------------------------------------------------')


# Define the DAG
dag = DAG(
    dag_id='GitlabEndDay',
    start_date=datetime(2023, 6, 6),
    schedule_interval=None
)

taskExtract = PythonOperator(
    task_id='ExtractCommits',
    python_callable=extract_commits,
    dag=dag
)

taskSentiment = PythonOperator(
    task_id='CalcSentiment',
    python_callable=print_sentiment,
    provide_context=True,
    dag=dag
)

taskFreq = PythonOperator(
    task_id='CalcFreq',
    python_callable=get_freq,
    provide_context=True,
    dag=dag
)

taskNotify = PythonOperator(
    task_id='Notify',
    python_callable=notify_slack,
    provide_context=True,
    dag=dag
)

# Define the task dependencies

taskExtract >> [taskSentiment, taskFreq]
[taskSentiment, taskFreq] >> taskNotify

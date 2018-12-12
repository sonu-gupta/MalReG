# Detecting and Analyzing Malicious Retweeter Groups

Sonu Gupta, Ponnurangam Kumaraguru, Tanmoy Chakraborty

The repository contains three feature extraction scripts used in our paper "MalReG: Detecting and Analyzing Malicious Retweeter Groups" ([accepted at CoDS-COMAD 2019](http://www.cods-comad.in/2019/index.html)). 

## Abstract

Given a retweeter network in Twitter for any event, how can we detect the group of users that collude to retweet together maliciously? A large number of retweets of a post often indicates the virality of the post. It also helps increase the visibility and volume of hashtags, topics or URLs, to promote the event associated with it. Our primary hunch is that there is synchronization or indicative pattern in the behavior of such users. In this paper, we propose (i) MalReG, a novel algorithm to detect retweeter groups, and (ii) a set of 23 group-based features (entropy-based and temporal-based) to train a supervised model to identify malicious retweeter groups (MRG). We present experiments on three real-world datasets with more than 10 million retweets crawled from Twitter. MalReG identifies 1, 017 retweeter groups present in our dataset. We train a supervised learning model to detect MRG which achieves 0.921 ROC AUC using Random Forest, outperforming the baseline by 7.97% higher AUC. Additionally, we perform geographical location-based and temporal analysis of these groups. Interestingly, we find the presence of the same group, retweeting different political events that took place in different continents at different times. We also discover masquerading techniques used by MRG to evade detection.

## Dataset

The annotated retweeter groups are available at https://tinyurl.com/yawz6z6h. Contact sonug@iiitd.ac.in for more details.

To cite, please use the following:

```latex
@inproceedings{gupta2019malreg,
  title={MalReG: Detecting and Analyzing Malicious Retweeter Groups},
  author={Gupta, Sonu and Kumaraguru, Ponnurangam and Chakraborty, Tanmoy},
  booktitle={Proceedings of the ACM India Joint International Conference on Data Science and Management of Data},
  year={2019},
  organization={ACM}
}
```

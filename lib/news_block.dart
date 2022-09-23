import 'dart:async';
import 'dart:convert';

import 'news_info.dart';
import 'package:http/http.dart' as http;

enum NewsAction {
  fetch,
  delete,
}

class NewsBloc {
  final _stateStreamController = StreamController<List<Article>>();
  StreamSink<List<Article>> get _newsSink => _stateStreamController.sink;
  Stream<List<Article>> get newsStream => _stateStreamController.stream;

  final _eventStreamController = StreamController<NewsAction>();
  StreamSink<NewsAction> get eventSink => _eventStreamController.sink;
  Stream<NewsAction> get _eventStream => _eventStreamController.stream;

  NewsBloc() {
    _eventStream.listen((event) async {
      if (event == NewsAction.fetch) {
        try {
          var news = await getNews();
          if (news != null)
            _newsSink.add(news.articles);
          else {
            _newsSink.addError('Something went wrong');
          }
        } on Exception catch (e) {
          _newsSink.addError('Something went wrong');
        }
      }
    });
  }
  Future<NewsModel> getNews() async {
    var client = http.Client();
    var newsModel;

    try {
      var response = await client.get(
          'http://newsapi.org/v2/everything?domains=twit.tv&apiKey=c4b01cd8445c45f48d3cb8b1a3ebf343');
      if (response.statusCode == 200) {
        var jsonString = response.body;
        var jsonMap = json.decode(jsonString);
        print(response.statusCode);
        print(response.body);
        newsModel = NewsModel.fromJson(jsonMap);
      }
    } catch (Exception) {
      return newsModel;
    }

    return newsModel;
  }
}

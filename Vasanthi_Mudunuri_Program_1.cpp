#define _GLIBCXX_USE_CXX11_ABI 0
#include <algorithm>
#include <limits>
#include <string>
#include  "stdint.h"
#include "Pipes.hh"
#include "TemplateFactory.hh"
#include "SerialUtils.hh"
#include "StringUtils.hh"

class MovieMapper : public HadoopPipes::Mapper {
 public:
  MovieMapper(HadoopPipes::TaskContext& context) {} //emitting moviename and moviegenre from mapper
  void map(HadoopPipes::MapContext& context) {
    std::string line = context.getInputValue();
    std::vector<std::string> record = HadoopUtils::splitString(line,"::");
    std::string moviename=record[1];
    std::string moviegenre=record[2];
    context.emit(HadoopUtils::toString(1),moviename+"::"+moviegenre);
    }
};

class MovieReducer : public HadoopPipes::Reducer {
 public:
  MovieReducer(HadoopPipes::TaskContext& context) {}  //calculating average number of movies per genres
  void reduce(HadoopPipes::ReduceContext& context) {
    std::vector<std::string> genrelist;
                        genrelist[0]="Action";
                        genrelist[1]="Adventure";
                        genrelist[2]="Animation";
                        genrelist[3]="Children's";
                        genrelist[4]="Comedy";
                        genrelist[5]="Crime";
                        genrelist[6]="Documentary";
                        genrelist[7]="Drama";
                        genrelist[8]="Fantasy";
                        genrelist[9]="Film-Noir";
                        genrelist[10]="Horror";
                        genrelist[11]="Musical";
                        genrelist[12]="Mystery";
                        genrelist[13]="Romance";
                        genrelist[14]="Sci-Fi";
                        genrelist[15]="Thriller";
                        genrelist[16]="War";
                        genrelist[17]="Western";
    std::vector<std::string> movienames;
    std::vector<std::string> genrenames;
    std::vector<std::string> moviegenre;
    int totalmovies=0;
    int counter=0;
    int i=0;
    int x=0;
    while(context.nextValue())
    {
    std::vector<std::string> record = HadoopUtils::splitString(context.getInputValue(),"::");
    movienames[i]=record[0];
    genrenames[i]=record[1];
    i++;
    }
    std::vector<std::string>::iterator it=movienames.begin();
    std::vector<std::string>::iterator end=movienames.end();
    for(;it<end;it++)
    {
        totalmovies++;
    }
    std::vector<std::string>::iterator ite=genrenames.begin();
    std::vector<std::string>::iterator last=genrenames.end();
    for(;ite<last;ite++)
    {
        std::vector<std::string> record1 = HadoopUtils::splitString(*ite,"|");
        std::vector<std::string>::iterator recordit=record1.begin();
        std::vector<std::string>::iterator recordend=record1.end();
        for(;recordit<recordend;recordit++)
        {
           moviegenre[x]=*recordit;
           x++;
        }
    }
    std::vector<std::string>::iterator genreite=genrelist.begin();
    std::vector<std::string>::iterator genreend=genrelist.end();
    for(;genreite<genreend;genreite++)
    {
        std::vector<std::string>::iterator movieite=moviegenre.begin();
        std::vector<std::string>::iterator movielast=moviegenre.end();
        for(;movieite<movielast;movieite++)
        {
            if(*movieite==*genreite)
            {
                counter++;
            }
        }
        context.emit(*genreite,HadoopUtils::toString(float(counter)/float(totalmovies)));
        counter=0;
    }
  }
};

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(
    HadoopPipes::TemplateFactory<MovieMapper, MovieReducer>()
  );
}

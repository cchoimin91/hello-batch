package com.choimin.urlshort.mapper;

import com.choimin.urlshort.model.UrlShortGetDto;
import com.choimin.urlshort.model.UrlShortSetDto;
import org.apache.ibatis.session.SqlSessionException;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

@Repository
public class UrlShortMapper {

    @Autowired
    private SqlSessionTemplate sqlSessionTemplate;


    public Integer existUrl(String originUrl) {
        return sqlSessionTemplate.selectOne("existUrl", originUrl);
    }

    public UrlShortGetDto getOriginUrl(String shortUrl){
        return sqlSessionTemplate.selectOne("getOriginUrl", shortUrl);
    }

    public long insertOriginUrl(UrlShortSetDto urlShortSetDto){
        return sqlSessionTemplate.insert("insertOriginUrl", urlShortSetDto);
    }

    public void updateShortUrl(UrlShortSetDto urlShortSetDto){
        sqlSessionTemplate.update("updateShortUrl", urlShortSetDto);
    }

    public String findByOriginUrl(String shortUrl){
        return sqlSessionTemplate.selectOne("findByOriginUrl", shortUrl);
    }





    public void insertTest() {
       sqlSessionTemplate.insert("testInsert");
    }

    public void updateTest() throws SQLException {
        throw new SQLException();
    }



}

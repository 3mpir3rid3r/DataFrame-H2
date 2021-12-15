package com.trading.bot.repository;

import com.trading.bot.entity.TableMst;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TableMstRepository extends JpaRepository<TableMst, Long> {
}

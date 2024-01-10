import time
import os
import pypeln as pl
from utils import fetch_domains, is_distinct, make_chunks
from seatable_api import Base, context


def map_row(base, table, rows, row_mapper):
    update = [
        {
            "row_id": row["_id"],
            "row": row_mapper(row)
        } for row in rows
    ]
    base.batch_update_rows(table, update)



server_url = context.server_url or 'https://cloud.seatable.cn'
api_token = context.api_token or os.environ["SEA_TABLE_TOKEN"]

base = Base(api_token, server_url)
base.auth()


def stage1_map_domain_shop(domain_shop):
    if "名称" not in domain_shop:
        return

    domain_shop_name = domain_shop["名称"]
    domain_shop_id = domain_shop["_id"]
    url = domain_shop["URL"]

    need_update = []
    need_add = []

    domain_shop_new = {}

    try:
        domains = fetch_domains(url)
        for domain in domains:
            if f"{domain}-{domain_shop_id}" in excludes_cached:
                print("skip", domain, domain_shop_id)
                continue


            domain_prefix = domain.split(".")[0]
            domain_row = {
                "域名": domain,
                "米表名称": domain_shop_name,
                "长度": len(domain.split(".")[0]),
                "前缀": domain_prefix,
                "后缀": domain.replace(domain_prefix, ""),
                "有效": True,
            }

            # 可能有bug
            exist_key = exist_domains_dn_id.get(domain)

            if exist_key:
                need_update.append({
                    "row_id": exist_key,
                    "row": domain_row
                })
                print(f"{domain_shop_name} {domain}已存在，需要更新")
            else:
                need_add.append(domain_row)
                print(f"{domain_shop_name} {domain}不存在，需要插入")

        domain_shop_new["上次更新时间"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        domain_shop_new["是否连通"] = True

    except Exception as e:
        domain_shop_new["是否连通"] = False
        print("failed", e)

    print(f"{domain_shop_name} +{len(need_add)} *{len(need_update)}")

    base.update_row("米表", domain_shop["_id"], domain_shop_new)

    base.batch_append_rows("域名", need_add)
    base.batch_update_rows("域名", need_update)

    exist_domains = base.query(f'select _id,域名 from 域名 where 米表名称 = "{domain_shop_name}" and 有效 = true')
    domain_id_list = list(map(lambda row: row["_id"], exist_domains))

    assert is_distinct(domain_id_list)

    print(f"{domain_shop_name} domains {len(domain_id_list)}")
    for chunk in make_chunks(list(domain_id_list), 50):
        base.batch_update_links(link_id=domain_rel_domain_shop_id,
                                table_name="域名",
                                other_table_name="米表",
                                row_id_list=chunk,
                                other_rows_ids_map={
                                    domain_id: [domain_shop_id] for domain_id in chunk
                                })


if __name__ == '__main__':

    print("先置所有域名为无效")
    domain_rows = base.list_rows("域名")
    map_row(base, "域名", domain_rows, lambda row: {"有效": False})

    # TODO: 域名不唯一，可能一个域名多家上线了，需要修改唯一约束
    exist_domains_dn_id = {r["域名"]: r["_id"] for r in domain_rows}

    domain_shop_rows = base.query("select _id, 名称, URL from 米表 where 是否抓取 = true and len(名称) > 0")
    excludes = base.query("select 域名, 米表 from 排除请求 where 有效 = true")
    print("excludes",excludes)

    excludes_cached = {
        f'{row["域名"]}-{row["米表"][0]["row_id"]}': True for row in excludes if len(row["米表"]) > 0
    }

    print(excludes_cached)

    domain_rel_domain_shop_id = base.get_column_link_id('域名', '米表')

    begin = time.time()

    stage = pl.thread.map(stage1_map_domain_shop, domain_shop_rows, workers=16, maxsize=16)

    data = list(stage)
    print(data)
    end = time.time()
    print(end - begin)

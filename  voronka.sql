with evs as ( --в этом блоке вытаскиваются название ивента и номер телефона, так же происходит нумерация строк
    select distinct
        regexp_match(event_json, '\":\"(\d.*)\"') as id,
        event_timestamp,
        appmetrica_device_id,
        event_datetime,
        event_name,
        regexp_match(event_json, '{\"(.*)\":{') as sphere,
        ROW_NUMBER() over () as string_number
    from yandex_app_metrika_logs_event_mus
    where
        event_datetime::date >= '2022-10-25'
),

evs_bookings as (
    select
        event_datetime,
        event_datetime::date as event_date,
        sphere,
        appmetrica_device_id,
        'mobile' as device_category,
        ROW_NUMBER() over() as ev_rn,
        SUM(
            case
                when id is not null and LENGTH(id::text) > 10 then 1
                else 0
            end
        ) as bookings
    from evs
    where event_name like 'Бронирование успешно выполнено'
    group by
        1, 2, 3, 4, 5
),

insts as ( --в этом блоке пронумировываются строки по appmetrica_device_id, отсортированные по времени инсталяции
    select
        install_timestamp,
        install_datetime,
        appmetrica_device_id,
        tracker_name as utm_source,
        case -- вытаскиваем название источников из навзваний трекеров
            when
                tracker_name ilike '%telegram%' and tracker_name not ilike '%smm%' then 'tg_ads'
            when 
                tracker_name ilike '%telegram_posev%' then 'tg_posev'
            when 
                tracker_name ilike '%mytarget%' then 'mytarget'
            when
                tracker_name ilike '%vk%' and tracker_name ilike '%twiga%' then 'vk_ads'
            when 
                tracker_name ilike '%vk_posev%' then 'vk_posev'
            when
                tracker_name ilike '%яндекс.директ%' then 'yandex'
            when 
                tracker_name ilike 'unknown' then 'organic'
            when
                tracker_name ilike '%PR%' or tracker_name ilike '%SMM%' then 'PR MUSbooking'
            when
                tracker_name ilike '%hendrix%' or tracker_name ilike '%hendrixstudio%' or 
                tracker_name ilike '%Танцевальные залы Санкт-Петербург (танцы)%' then 'hendrixstudio.ru'
            when 
                tracker_name ilike '%Musbooking%' then 'musbooking'
            else tracker_name
        end as source,
        case -- разделяю рекламу на mobile и web qr
            when tracker_name ilike '%QR%' or tracker_name ilike '%QR-%' then 'web QR'
            else 'mobile'
        end as ad_category,
        COUNT(install_timestamp) as installs, -- считаем установки
        ROW_NUMBER() over (
            partition by appmetrica_device_id order by install_timestamp
        ) as number
    from yandex_app_metrika_logs_installation_mus
    where install_datetime::date >= '2022-01-01'
    group by
        1, 2, 3, 4
),

insts_grouped as ( -- суммирую установку по дням, источникам и типам рекламя 
    select
        install_datetime::date as date,
        source,
        ad_category,
        SUM(installs) as installs
    from insts
    where install_datetime::date >= '2022-10-25'
    group by 1, 2, 3
),

clicks as ( -- с кликами такие же манипуляции как и с установками
    select
        click_datetime,
        tracker_name,
        case
            when
                tracker_name ilike '%telegram%' and tracker_name not ilike '%smm%' then 'tg_ads'
            when 
                tracker_name ilike '%telegram_posev%' then 'tg_posev'
            when 
                tracker_name ilike '%mytarget%' then 'mytarget'
            when
                tracker_name ilike '%vk%' and tracker_name not ilike '%hendrix%' then 'vk_ads'
            when 
                tracker_name ilike '%vk_posev%' then 'vk_posev'
            when
                tracker_name ilike '%яндекс.директ%'  then 'yandex'
            when 
                tracker_name ilike 'unknown' then 'organic'
            when
                tracker_name ilike '%PR%' or tracker_name ilike '%SMM%' then 'PR MUSbooking'
            when
                tracker_name ilike '%hendrix%' or tracker_name ilike '%hendrixstudio%' or 
                
                tracker_name ilike '%Танцевальные залы Санкт-Петербург (танцы)%' then 'hendrixstudio.ru'
            when 
                tracker_name ilike '%Musbooking%' then 'musbooking'
            else tracker_name
        end as source,
        case
            when tracker_name ilike '%QR%' or tracker_name ilike '%QR-%' then 'web QR'
            else 'mobile'
        end as ad_category
    from yandex_app_metrika_logs_clicks
    where click_datetime::date >= '2022-10-25'--) select * from clicks where source like 'mytarget';
),

clicks_grouped as ( -- 1 в 1 как с установками
    select
        click_datetime::date as click_date,
        source,
        ad_category,
        COUNT(click_datetime) as clicks
    from clicks
    group by
        1, 2, 3--) select sum(clicks) from clicks_grouped where source like 'mytarget' and ad_category like 'mobile';
),

evs_installs as ( -- соединяю установки и события для последующей группировки
    select
        event_date,
        sphere,
        source,
        ad_category,
        bookings,
        ROW_NUMBER() over( -- для последующего удаления дублирующихся строк
            partition by
                ev_rn
            order by evs_bookings.event_date
        ) as rn
    from evs_bookings
    left join insts
        on evs_bookings.appmetrica_device_id = insts.appmetrica_device_id
),

evs_grouped as (  --суммирую брони по дням, источникам и типу рекламы
    select
        event_date,
        sphere,
        source,
        ad_category,
        SUM(COALESCE(bookings, 0)) as bookings
    from evs_installs
    where rn = 1
    group by
        1, 2, 3, 4
),

evs_grouped_installs as ( -- присоединяю остальные установки, по которым не было бронирований
    select
        sphere,
        bookings,
        installs,
        coalesce(event_date, date) as date,
        case
            when event_date is null then insts_grouped.source
            else evs_grouped.source
        end as source,
        case
            when event_date is null then insts_grouped.ad_category
            else evs_grouped.ad_category
        end as ad_category
    from evs_grouped
    full join insts_grouped
        on event_date = date
            and evs_grouped.source = insts_grouped.source
            and evs_grouped.ad_category = insts_grouped.ad_category
),

installs_bookings as ( -- промежуточный этап для большей ясности
    select
        date,
        sphere,
        source,
        ad_category,
        installs,
        COALESCE(bookings, 0) as bookings
    from evs_grouped_installs
),

events_installs as ( --в этом блоке происходит джойн ивентов и инсталляций для дальнейшего юниона с метрикой
    select
        evs.appmetrica_device_id,
        evs.event_timestamp,
        evs.id,
        evs.sphere,
        evs.string_number,
        evs.event_datetime,
        evs.event_name,
        'mobile' as device_category,
        number,
        insts.install_datetime,
        insts.install_timestamp,
        insts.source,
        insts.ad_category,
        -- эта функция удаляет дубликаты строк после джойне ивентов и установок
        ROW_NUMBER() over(
            partition by
                evs.appmetrica_device_id,
                evs.event_timestamp,
                evs.event_name,
                string_number
            order by evs.appmetrica_device_id
        ) as rn
    from evs
    left join insts
        on evs.appmetrica_device_id = insts.appmetrica_device_id
),

attribution as ( --в этом блоке считается атрибуция
    select
        id,
        sphere,
        event_name,
        event_datetime,
        device_category,
        source,
        ad_category,
        NULL as traffic_source,
        case
            when
                number = MIN(
                    number
                ) over (
                    partition by string_number
                ) and event_timestamp::text >= install_timestamp::text then 'first'
            when
                number = MIN(
                    number
                ) over (
                    partition by string_number
                ) and event_timestamp::text < install_timestamp::text then 'date incorrect'
            when number is null then 'not_match'
            else 'none'
        end as first_t
    from events_installs
    where rn = 1
    group by
        string_number,
        event_timestamp,
        install_timestamp,
        id,
        ad_category,
        sphere,
        event_name,
        device_category,
        number,
        source,
        event_datetime
),

appmetrica_metrika as ( --в этом блоке происходит union данных из яндекс метрики
    select
        *,
        event_datetime::date as date,
        NULL as bookings
    from attribution
    where
        first_t not like 'none'
        and event_name like 'Бронирование успешно выполнено' and
        LENGTH(id) > 10 and id is not null -- оставляю только id транзаций
    union all
    select
        id,
        NULL as sphere,
        NULL as event_name,
        NULL as event_datetime,
        device_category,
        case
            when utm_source like 'yandex' then 'yandex'
            when utm_source ilike '%vk%' then 'vk_ads'
            when utm_source ilike '%vk_posev%' then 'vk_posev'
            when utm_source ilike '%mytarget%' then 'mytarget'
            when utm_source ilike '%telegram%' then 'tg_ads'
            when utm_source ilike '%telegram_posev%' then 'tg_posev'
            when utm_source is null then traffic_source
            else utm_source
        end as source,
        'web' as ad_category,
        traffic_source,
        'first_metrika' as first_t,
        date::date as date,
        SUM(
            case
                when id is not null then 1
                else 0
            end
        ) as bookings -- считаю количество броней в яндекс метрике
    from metrika_utm_ids
    -- оставляю строки с момента старта рекламы и только строки с id заказа
    where date >= '2022-10-25' and id is not null
    group by
        id,
        device_category,
        utm_source,
        traffic_source,
        date::date--) select * from appmetrica_metrika where first_t like 'first_metrika';
),

ads as ( -- группировка рекламных кликов и расходов по дням,источникам и кампаниям
    select
        date::date as date,
        source,
        case
            when
                campaign ilike '%ios%' or campaign ilike '%android%' then 'mobile'
            else 'web' -- столбец который содержит инфу, к какой платформе относятся клики и расходы (веб или приложение)
        end as ad_category,
        SUM(clicks::numeric) as clicks,
        SUM(cost::numeric) as cost
    from clicks_cost
    group by
        date,
        source,
        campaign
),

ads_grouped as ( -- дополнительный запрос, чтобы убрать группировку по кампаниям
    select
        date,
        source,
        ad_category,
        SUM(clicks) as clicks,
        SUM(cost) as cost
    from ads
    where date <= (select CURRENT_DATE::date)
    group by
        1, 2, 3--) select sum(clicks) from ads_grouped where source like 'yandex' and ad_category like 'mobile';

), t1 as ( --в этом блоке начинается работа с данными из СРМ
    select
        id,
        date::date as keydate,
        status,
        row_number() over(
            partition by id, date::date, status order by id
        ) as rn,
        case --добавляется нейминг для площадок (сфер)
            when
                basesphere like '0676eaa9-4ddb-495c-8980-06250d7d5f4a' then 'Репетиционные базы'
            when
                basesphere like '38f7aeac-d462-41a2-b4e7-f2c642cb9225' then 'Музыкальные классы'
            when
                basesphere like '36e5c7bc-f45c-4251-9552-456664c55c22' then 'Танцевальные залы'
            when
                basesphere like 'b2c8f9ff-a0f0-42f8-be3e-c061d5c1144c' then 'Фотостудии'
            when
                basesphere like '0be020e3-34dd-432b-ba93-4a7cc45a2777' then 'Площадки для мероприятий'
            when
                basesphere like '41ce174b-2964-416b-9830-3468bf15dba3' then 'Студии звукозаписи'
            when
                basesphere like 'e6d1f71b-f1a0-4686-9279-08547b248c18' then 'Школы и педагоги'
            else 'нет'
        end as sphere,
        case --считается комиссия для каждой площадки 
            when
                status::numeric = 10 and basesphere like '0676eaa9-4ddb-495c-8980-06250d7d5f4a' then (
                    total::numeric * 0.124
                )
            when
                status::numeric = 10 and basesphere like '38f7aeac-d462-41a2-b4e7-f2c642cb9225' then (
                    total::numeric * 0.106
                )
            when
                status::numeric = 10 and basesphere like '36e5c7bc-f45c-4251-9552-456664c55c22' then (
                    total::numeric * 0.125
                )
            when
                status::numeric = 10 and basesphere like 'b2c8f9ff-a0f0-42f8-be3e-c061d5c1144c' then (
                    total::numeric * 0.099
                )
            when
                status::numeric = 10 and basesphere like '0be020e3-34dd-432b-ba93-4a7cc45a2777' then (
                    total::numeric * 0.1
                )
            when
                status::numeric = 10 and basesphere like '41ce174b-2964-416b-9830-3468bf15dba3' then (
                    total::numeric * 0.1
                )
            when
                status::numeric = 10 and basesphere like 'e6d1f71b-f1a0-4686-9279-08547b248c18' then (
                    total::numeric * 0.097
                )
            else 0
        end as comission,
        --здесь считаются количество и суммы по заказам (включая отмененные)
        case
            when status::numeric = 10 then count(id)
            else 0
        end as closed,
        case
            when status::numeric = 11 then count(id)
            else 0
        end as cancel,
        case
            when status::numeric = 10 then sum(total::numeric)
            else 0
        end as closed_total,
        case
            when status::numeric = 11 then sum(total::numeric)
            else 0
        end as cancel_total,
        sum(total::numeric) as total
    from crmtable
    where
        date::date >= '2022-01-01'
    group by
        id,
        keydate,
        status,
        sourcetype,
        comission,
        basesphere
),

crm as ( --в этом блоке отфильтровываются возможные задвоения в CRM
    select
        id,
        keydate,
        sphere,
        status,
        comission,
        closed,
        cancel,
        closed_total,
        cancel_total,
        total
    from t1
    where rn = 1
    order by keydate
),

joined as ( --в этом блоке соединяются данные из систем веб-аналитики и CRM
    select
        appmetrica_metrika.id,
        appmetrica_metrika.date,
        source,
        ad_category,
        bookings,
        closed,
        cancel,
        closed_total,
        cancel_total,
        total,
        comission,
        coalesce(appmetrica_metrika.sphere, crm.sphere) as spheres
    from appmetrica_metrika
    left join crm --джойн по id заказа
        on
            appmetrica_metrika.id = crm.id
),

joined_w_bi as ( -- здесь я приклеиваю установки и брони из таблицы installs_bookings
    select
        installs,
        joined.bookings as ym_bookings,
        installs_bookings.bookings as am_bookings,
        closed,
        cancel,
        closed_total,
        cancel_total,
        total,
        comission,
        coalesce(joined.date, installs_bookings.date) as date,
        coalesce(joined.spheres, installs_bookings.sphere) as spheres,
        coalesce(joined.source, installs_bookings.source) as source,
        coalesce(joined.ad_category,
        installs_bookings.ad_category) as ad_category
    from joined
    full join installs_bookings
        on joined.date = installs_bookings.date
            and joined.spheres = installs_bookings.sphere
            and joined.source = installs_bookings.source
            and joined.ad_category = installs_bookings.ad_category
),

metrika_visits as ( -- подготовка данных метрики для последующего джойна визитов (кликов) к общей таблице
    select
        date::date as date,
        'web' as ad_category,
        visits,
        case
            when utm_source ilike '%yandex%' then 'yandex'
            when utm_source ilike '%vk%' then 'vk_ads'
            when utm_source ilike '%vk_posev%' then 'vk_posev'
            when utm_source ilike '%mytarget%' then 'mytarget'
            when utm_source ilike '%telegram%' then 'tg_ads'
            when utm_source ilike '%telegram_posev%' then 'tg_posev'
            when utm_source is null then traffic_source
            else utm_source
        end as source
    from metrika_utm_ids
    where date >= '2022-10-25'  -- оставляю строки с момента старта рекламы
),

metrika_am_grouped as ( -- здесь суммирую вищиты (клики) по дням, источникам и типам рекламы и делаю юнион кликов из апметрики
    select
        date,
        source,
        ad_category,
        sum(visits::numeric) as clicks
    from metrika_visits
    group by
        1, 2, 3
    union all
    select *
    from clicks_grouped
),

add_clicks as ( -- здесь джойн кликов с прошлого шага к общим данным
    select
        spheres,
        clicks,
        installs,
        ym_bookings,
        am_bookings,
        closed,
        cancel,
        closed_total,
        cancel_total,
        total,
        comission,
        coalesce(joined_w_bi.date, metrika_am_grouped.date) as date,
        coalesce(joined_w_bi.source, metrika_am_grouped.source) as source,
        coalesce(joined_w_bi.ad_category,
        metrika_am_grouped.ad_category) as ad_category
    from joined_w_bi
    full join metrika_am_grouped
        on joined_w_bi.date = metrika_am_grouped.date
            and joined_w_bi.source = metrika_am_grouped.source
            and joined_w_bi.ad_category = metrika_am_grouped.ad_category
),

add_cost as ( -- на этом шаге добавляю расходы и рекламные клики к общим данным
    select
        spheres,
        add_clicks.clicks,
        ads_grouped.clicks as ad_clicks,
        installs,
        ym_bookings,
        am_bookings,
        cost,
        closed,
        cancel,
        closed_total,
        cancel_total,
        total,
        comission,
        coalesce(add_clicks.date, ads_grouped.date) as date,
        coalesce(add_clicks.source, ads_grouped.source) as source,
        coalesce(add_clicks.ad_category, ads_grouped.ad_category) as ad_category
    from add_clicks
    full join ads_grouped
        on add_clicks.date = ads_grouped.date::date
            and add_clicks.source = ads_grouped.source
            and add_clicks.ad_category = ads_grouped.ad_category
),

enumerate_duplicated_rows as (-- в этом блоке пронумировываю строки, которые задвоились после джойнов
    select
        *,
        -- ROW_NUMBER() OVER(
        --     PARTITION BY date, source, ad_category, ym_bookings
        --     ORDER BY date
        -- ) as rn_ym_bookings,       
        row_number() over(
            partition by date, source, ad_category, installs
            order by date
        ) as rn_installs,
        row_number() over(
            partition by date, spheres, source, ad_category, am_bookings
            order by date
        ) as rn_am_bookings,
        row_number() over(
            partition by date, source, ad_category, clicks
            order by date, source
        ) as rn_clicks,
        row_number() over(
            partition by date, source, ad_category, ad_clicks
            order by date, source
        ) as rn_ad_clicks,
        row_number() over(
            partition by date, source, ad_category, cost
            order by date, source
        ) as rn_cost
    from add_cost
),

divide_duplicates as ( -- здесь я делю задублированные значения
    select
        date,
        source,
        ad_category,
        spheres,
        ym_bookings,
        closed,
        cancel,
        closed_total,
        cancel_total,
        total,
        comission,
        round(
            clicks / count(
                rn_clicks
            ) over (partition by date, source, ad_category, clicks),
            2
        ) as clicks,
        round(
            ad_clicks / count(
                rn_ad_clicks
            ) over (partition by date, source, ad_category, ad_clicks),
            2
        ) as ad_clicks,
        round(
            installs / count(
                rn_installs
            ) over (partition by date, source, ad_category, installs),
            2
        ) as installs,
        round(
            am_bookings / count(
                rn_am_bookings
            ) over (
                partition by date, spheres, source, ad_category, am_bookings
            ),
            2
        ) as am_bookings,
        round(
            cost / count(
                rn_cost
            ) over (partition by date, source, ad_category, cost),
            2
        ) as cost
    from enumerate_duplicated_rows
),

polish_everything as ( -- здесь запрос приводится в порядок в плане своей презентабельности
    select
        date,
        coalesce(source, 'Не определено') as source,
        case
            when source in (
                'yandex', 'mytarget', 'vk_ads', 'vk_posev')
                then 'Реклама'
            else 'Не реклама'
        end as ad_no_ad,
        coalesce(ad_category, 'Не определено') as ad_category,
        coalesce(spheres, 'Не определена') as spheres,
        round(sum(coalesce(clicks, 0)), 0) as clicks,
        round(sum(coalesce(ad_clicks, 0)), 0) as ad_clicks,
        round(sum(coalesce(installs, 0))) as installs,
        round(
            sum(coalesce(ym_bookings, 0)) + sum(coalesce(am_bookings, 0))
        ) as bookings,
        round(sum(coalesce(cost, 0)), 0) as cost,
        sum(coalesce(closed, 0)) as closed,
        sum(coalesce(cancel, 0)) as cancel,
        sum(coalesce(closed_total, 0)) as closed_total,
        sum(coalesce(cancel_total, 0)) as cancel_total,
        sum(coalesce(total, 0)) as total,
        sum(coalesce(comission, 0)) as comission
    from divide_duplicates
    group by
        1, 2, 3, 4, 5
),

translate_sources as ( -- здесь происходит перевод названий источников на русский язык
    select
        date,
        ad_no_ad,
        ad_category,
        spheres,
        clicks,
        ad_clicks,
        installs,
        bookings,
        cost,
        cancel,
        closed,
        closed_total,
        cancel_total,
        total,
        comission,
        case
            when source like 'vk_ads' then 'Вконтакте'
            when source like 'vk_posev' then 'Вконтакте Посевы'
            when source like 'mytarget' then 'МайТаргет'
            when source like 'yandex' then 'Яндекс'
            when source like 'tg_ads' then 'Телеграм'
            when source like 'tg_posev' then 'Телеграм Посевы'
            when source like 'organic' then 'Органика'
            when source like 'other' then 'Другой источник'
            when source like 'direct' then 'Прямой переход'
            when source like 'internal' then 'Внутренние переходы'
            when source like 'referral' then 'Ссылки на сайтах'
            when source like 'social' then 'Социальные сети'
            when source like 'ad' then 'Рекламный трафик'
            when source like 'messenger' then 'Мессенджеры'
            else source
        end as source
    from polish_everything
)

select 
        date,
        source,
        ad_no_ad,
        ad_category,
        spheres,
        clicks::int,
        ad_clicks::int,
        installs::int,
        bookings::int,
        cost::int,
        cancel::int,
        closed::int,
        closed_total::int,
        cancel_total::int,
        total::int,
        comission::int
from translate_sources
where date < (select current_date::date);






select cd_transacao, dm_usuario_sk, count(*)
from {{ref('ft_transacoes_usuarios')}}
group by cd_transacao, cd_usuario
having count(*) > 1
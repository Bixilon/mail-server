/*
 * Copyright (c) 2023 Stalwart Labs Ltd.
 *
 * This file is part of Stalwart Mail Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use common::Core;

use store::Stores;
use utils::config::Config;

use smtp::core::{Inner, Session};

use crate::smtp::{
    build_smtp,
    session::{TestSession, VerifyResponse},
    TempDir,
};

const CONFIG: &str = r#"
[storage]
data = "sqlite"
lookup = "sqlite"
blob = "sqlite"
fts = "sqlite"

[store."sqlite"]
type = "sqlite"
path = "{TMP}/data.db"

[directory."local"]
type = "memory"

[[directory."local".principals]]
name = "john"
description = "John Doe"
secret = "secret"
email = ["john@foobar.org"]
email-list = ["sales@foobar.org"]

[[directory."local".principals]]
name = "jane"
description = "Jane Doe"
secret = "p4ssw0rd"
email = "jane@foobar.org"
email-list = ["sales@foobar.org"]

[[directory."local".principals]]
name = "bill"
description = "Bill Foobar"
secret = "p4ssw0rd"
email = "bill@foobar.org"
email-list = ["sales@foobar.org"]

[session.rcpt]
directory = "'local'"

[session.extensions]
vrfy = [{if = "remote_ip = '10.0.0.1'", then = true},
        {else = false}]
expn = [{if = "remote_ip = '10.0.0.1'", then = true},
        {else = false}]

"#;

#[tokio::test]
async fn vrfy_expn() {
    let tmp_dir = TempDir::new("smtp_vrfy_test", true);
    let mut config = Config::new(tmp_dir.update_config(CONFIG)).unwrap();
    let stores = Stores::parse_all(&mut config).await;
    let core = Core::parse(&mut config, stores, Default::default()).await;

    // EHLO should not advertise VRFY/EXPN to 10.0.0.2
    let mut session = Session::test(build_smtp(core, Inner::default()));
    session.data.remote_ip_str = "10.0.0.2".to_string();
    session.eval_session_params().await;
    session
        .ehlo("mx.foobar.org")
        .await
        .assert_not_contains("EXPN")
        .assert_not_contains("VRFY");
    session.cmd("VRFY john", "252 2.5.1").await;
    session.cmd("EXPN sales@foobar.org", "252 2.5.1").await;

    // EHLO should advertise VRFY/EXPN for 10.0.0.1
    session.data.remote_ip_str = "10.0.0.1".to_string();
    session.eval_session_params().await;
    session
        .ehlo("mx.foobar.org")
        .await
        .assert_contains("EXPN")
        .assert_contains("VRFY");

    // Successful VRFY
    session.cmd("VRFY john", "250 john@foobar.org").await;

    // Successful EXPN
    session
        .cmd("EXPN sales@foobar.org", "250")
        .await
        .assert_contains("250-john@foobar.org")
        .assert_contains("250-jane@foobar.org")
        .assert_contains("250 bill@foobar.org");

    // Non-existent VRFY
    session.cmd("VRFY robert", "550 5.1.2").await;

    // Non-existent EXPN
    session.cmd("EXPN procurement", "550 5.1.2").await;
}

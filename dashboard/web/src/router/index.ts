import { createRouter, createWebHistory } from "vue-router";
import { useAuthStore } from "@/stores/auth";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/login",
      name: "login",
      component: () => import("@/pages/LoginPage.vue"),
      meta: { public: true },
    },
    {
      path: "/",
      component: () => import("@/components/Layout.vue"),
      children: [
        {
          path: "",
          name: "apps",
          component: () => import("@/pages/AppsPage.vue"),
        },
        {
          path: "apps/:id",
          name: "app-detail",
          component: () => import("@/pages/AppDetailPage.vue"),
        },
        {
          path: "metrics",
          name: "metrics",
          component: () => import("@/pages/MetricsPage.vue"),
        },
        {
          path: "users",
          name: "users",
          component: () => import("@/pages/UsersPage.vue"),
          meta: { admin: true },
        },
      ],
    },
  ],
});

router.beforeEach(async (to) => {
  const auth = useAuthStore();
  if (!auth.email && !to.meta.public) {
    await auth.bootstrap();
  }
  if (!to.meta.public && !auth.email) {
    return { name: "login", query: { redirect: to.fullPath } };
  }
  if (to.meta.admin && !auth.isAdmin) {
    return { name: "apps" };
  }
  if (to.name === "login" && auth.email) {
    return { name: "apps" };
  }
});

export default router;
